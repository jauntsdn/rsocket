/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jauntsdn.rsocket;

import static com.jauntsdn.rsocket.StreamErrorMappers.*;
import static com.jauntsdn.rsocket.internal.ClientSetup.DefaultClientSetup;
import static com.jauntsdn.rsocket.internal.ClientSetup.ResumableClientSetup;

import com.jauntsdn.rsocket.exceptions.InvalidSetupException;
import com.jauntsdn.rsocket.exceptions.RejectedSetupException;
import com.jauntsdn.rsocket.fragmentation.FragmentationDuplexConnection;
import com.jauntsdn.rsocket.frame.*;
import com.jauntsdn.rsocket.frame.decoder.PayloadDecoder;
import com.jauntsdn.rsocket.internal.ClientServerInputMultiplexer;
import com.jauntsdn.rsocket.internal.ClientSetup;
import com.jauntsdn.rsocket.internal.ServerSetup;
import com.jauntsdn.rsocket.keepalive.KeepAliveHandler;
import com.jauntsdn.rsocket.plugins.*;
import com.jauntsdn.rsocket.resume.*;
import com.jauntsdn.rsocket.transport.ClientTransport;
import com.jauntsdn.rsocket.transport.ServerTransport;
import com.jauntsdn.rsocket.util.EmptyPayload;
import com.jauntsdn.rsocket.util.MultiSubscriberRSocket;
import com.jauntsdn.rsocket.util.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

/** Factory for creating RSocket clients and servers. */
public class RSocketFactory {
  /**
   * Creates a factory that establishes client connections to other RSockets.
   *
   * @return a client factory
   */
  public static ClientRSocketFactory connect() {
    return new ClientRSocketFactory();
  }

  /**
   * Creates a factory that receives server connections from client RSockets.
   *
   * @return a server factory.
   */
  public static ServerRSocketFactory receive() {
    return new ServerRSocketFactory();
  }

  public interface Start<T extends Closeable> {
    Mono<T> start();
  }

  public interface ClientTransportAcceptor {
    Start<RSocket> transport(Supplier<ClientTransport> transport);

    default Start<RSocket> transport(ClientTransport transport) {
      return transport(() -> transport);
    }
  }

  public interface ServerTransportAcceptor {

    <T extends Closeable> Start<T> transport(Supplier<ServerTransport<T>> transport);

    default <T extends Closeable> Start<T> transport(ServerTransport<T> transport) {
      return transport(() -> transport);
    }
  }

  public static class ClientRSocketFactory implements ClientTransportAcceptor {
    private static final String CLIENT_TAG = "client";
    private static final int KEEPALIVE_MIN_INTERVAL_MILLIS = 100;

    private SocketAcceptor acceptor = (setup, sendingSocket) -> Mono.just(new AbstractRSocket() {});

    private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private StreamErrorMappers errorMappers = StreamErrorMappers.create();
    private PluginRegistry plugins = new PluginRegistry(Plugins.defaultPlugins());

    private Payload setupPayload = EmptyPayload.INSTANCE;
    private PayloadDecoder payloadDecoder = PayloadDecoder.ZERO_COPY;

    private Duration tickPeriod = Duration.ofSeconds(15);
    private Duration ackTimeout = Duration.ofSeconds(90);

    private String metadataMimeType = "application/binary";
    private String dataMimeType = "application/binary";

    private boolean resumeEnabled;
    private boolean resumeCleanupStoreOnKeepAlive;
    private Supplier<ByteBuf> resumeTokenSupplier = ResumeFrameFlyweight::generateResumeToken;
    private Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory =
        token -> new InMemoryResumableFramesStore(CLIENT_TAG, 100_000);
    private Duration resumeSessionDuration = Duration.ofMinutes(2);
    private Duration resumeStreamTimeout = Duration.ofSeconds(10);
    private Supplier<ResumeStrategy> resumeStrategySupplier =
        () ->
            new ExponentialBackoffResumeStrategy(Duration.ofSeconds(1), Duration.ofSeconds(16), 2);

    private boolean multiSubscriberRequester = true;
    private boolean leaseEnabled;
    private Leases.ClientConfigurer leaseConfigurer = (rtt, scheduler) -> Leases.create();

    private ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private boolean acceptFragmentedFrames;
    private int frameSizeLimit = FrameLengthFlyweight.FRAME_LENGTH_MASK;

    public ClientRSocketFactory byteBufAllocator(ByteBufAllocator allocator) {
      Objects.requireNonNull(allocator);
      this.allocator = allocator;
      return this;
    }

    public ClientRSocketFactory addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
      plugins.addConnectionPlugin(interceptor);
      return this;
    }

    public ClientRSocketFactory addRequesterPlugin(RSocketInterceptor interceptor) {
      plugins.addRequesterPlugin(interceptor);
      return this;
    }

    public ClientRSocketFactory addResponderPlugin(RSocketInterceptor interceptor) {
      plugins.addResponderPlugin(interceptor);
      return this;
    }

    public ClientRSocketFactory addSocketAcceptorPlugin(SocketAcceptorInterceptor interceptor) {
      plugins.addSocketAcceptorPlugin(interceptor);
      return this;
    }

    public ClientRSocketFactory keepAlive(Duration tickPeriod, Duration ackTimeout) {
      this.tickPeriod = tickPeriod;
      this.ackTimeout = ackTimeout;
      return this;
    }

    public ClientRSocketFactory keepAliveTickPeriod(Duration tickPeriod) {
      this.tickPeriod = tickPeriod;
      return this;
    }

    public ClientRSocketFactory keepAliveAckTimeout(Duration ackTimeout) {
      this.ackTimeout = ackTimeout;
      return this;
    }

    public ClientRSocketFactory mimeType(String metadataMimeType, String dataMimeType) {
      this.dataMimeType = dataMimeType;
      this.metadataMimeType = metadataMimeType;
      return this;
    }

    public ClientRSocketFactory dataMimeType(String dataMimeType) {
      this.dataMimeType = dataMimeType;
      return this;
    }

    public ClientRSocketFactory metadataMimeType(String metadataMimeType) {
      this.metadataMimeType = metadataMimeType;
      return this;
    }

    /**
     * Enable and configure requests lease support
     *
     * @param leaseConfigurer configures requests lease
     * @return this {@link ClientRSocketFactory} instance
     */
    public ClientRSocketFactory lease(Leases.ClientConfigurer leaseConfigurer) {
      this.leaseEnabled = true;
      this.leaseConfigurer = Objects.requireNonNull(leaseConfigurer, "leaseConfigurer");
      return this;
    }

    /**
     * Enables requests lease support. Responder will not accept any requests because {@link Leases}
     * is not configured
     *
     * @return this {@link ClientRSocketFactory} instance
     */
    public ClientRSocketFactory lease() {
      this.leaseEnabled = true;
      return this;
    }

    /**
     * Enables requests lease support. Responder will not accept any requests because {@link Leases}
     * is not configured
     *
     * @return this {@link ClientRSocketFactory} instance
     */
    public ClientRSocketFactory lease(boolean leaseEnabled) {
      this.leaseEnabled = leaseEnabled;
      return this;
    }

    public ClientRSocketFactory singleSubscriberRequester() {
      return singleSubscriberRequester(true);
    }

    public ClientRSocketFactory singleSubscriberRequester(boolean singleSubscriberRequester) {
      this.multiSubscriberRequester = !singleSubscriberRequester;
      return this;
    }

    public ClientRSocketFactory resume() {
      return resume(true);
    }

    public ClientRSocketFactory resume(boolean resumeEnabled) {
      this.resumeEnabled = resumeEnabled;
      return this;
    }

    public ClientRSocketFactory resumeToken(Supplier<ByteBuf> resumeTokenSupplier) {
      this.resumeTokenSupplier = Objects.requireNonNull(resumeTokenSupplier);
      return this;
    }

    public ClientRSocketFactory resumeStore(
        Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory) {
      this.resumeStoreFactory = resumeStoreFactory;
      return this;
    }

    public ClientRSocketFactory resumeSessionDuration(Duration sessionDuration) {
      this.resumeSessionDuration = Objects.requireNonNull(sessionDuration);
      return this;
    }

    public ClientRSocketFactory resumeStreamTimeout(Duration resumeStreamTimeout) {
      this.resumeStreamTimeout = Objects.requireNonNull(resumeStreamTimeout);
      return this;
    }

    public ClientRSocketFactory resumeStrategy(Supplier<ResumeStrategy> resumeStrategy) {
      this.resumeStrategySupplier = Objects.requireNonNull(resumeStrategy);
      return this;
    }

    public ClientRSocketFactory resumeCleanupOnKeepAlive() {
      resumeCleanupStoreOnKeepAlive = true;
      return this;
    }

    public ClientRSocketFactory frameSizeLimit(int frameSizeLimit) {
      this.frameSizeLimit = Preconditions.requireFrameSizeValid(frameSizeLimit);
      return this;
    }

    @Override
    public Start<RSocket> transport(Supplier<ClientTransport> transportClient) {
      return new StartClient(transportClient);
    }

    public ClientTransportAcceptor acceptor(Function<RSocket, RSocket> acceptor) {
      return acceptor(() -> acceptor);
    }

    public ClientTransportAcceptor acceptor(Supplier<Function<RSocket, RSocket>> acceptor) {
      return acceptor((setup, sendingSocket) -> Mono.just(acceptor.get().apply(sendingSocket)));
    }

    public ClientTransportAcceptor acceptor(SocketAcceptor acceptor) {
      this.acceptor = acceptor;
      return StartClient::new;
    }

    public ClientRSocketFactory acceptFragmentedFrames() {
      this.acceptFragmentedFrames = true;
      return this;
    }

    public ClientRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer) {
      this.errorConsumer = errorConsumer;
      return this;
    }

    /**
     * @param errorMappers configures custom error mappers of incoming and outgoing streams.
     * @return this {@link ClientRSocketFactory} instance
     */
    public ClientRSocketFactory streamErrorMapper(StreamErrorMappers errorMappers) {
      this.errorMappers = errorMappers;
      return this;
    }

    public ClientRSocketFactory setupPayload(Payload payload) {
      this.setupPayload = payload;
      return this;
    }

    public ClientRSocketFactory frameDecoder(PayloadDecoder payloadDecoder) {
      this.payloadDecoder = payloadDecoder;
      return this;
    }

    private class StartClient implements Start<RSocket> {
      private final Supplier<ClientTransport> transportClient;
      private final int keepAliveTickPeriod;
      private final int keepAliveTimeout;

      StartClient(Supplier<ClientTransport> transportClient) {
        this.transportClient = transportClient;
        this.keepAliveTickPeriod = keepAliveTickPeriod();
        this.keepAliveTimeout = keepAliveTimeout();
      }

      @Override
      public Mono<RSocket> start() {
        return newConnection()
            .flatMap(
                connection -> {
                  if (acceptFragmentedFrames) {
                    connection =
                        new FragmentationDuplexConnection(connection, allocator, frameSizeLimit);
                  }

                  ClientSetup clientSetup = clientSetup(connection);
                  boolean isLeaseEnabled = leaseEnabled;
                  ByteBuf resumeToken = clientSetup.resumeToken();

                  ByteBuf setupFrame =
                      SetupFrameFlyweight.encode(
                          allocator,
                          isLeaseEnabled,
                          keepAliveTickPeriod,
                          keepAliveTimeout,
                          resumeToken,
                          metadataMimeType,
                          dataMimeType,
                          setupPayload);

                  ConnectionSetupPayload connectionSetupPayload =
                      ConnectionSetupPayload.create(setupFrame);

                  KeepAliveHandler keepAliveHandler = clientSetup.keepAliveHandler();
                  DuplexConnection wrappedConnection = clientSetup.connection();

                  ClientServerInputMultiplexer multiplexer =
                      new ClientServerInputMultiplexer(wrappedConnection, plugins, true);

                  RSocketsFactory rSocketsFactory =
                      RSocketsFactory.createClient(
                          connectionSetupPayload.willClientHonorLease(),
                          connection.scheduler(),
                          leaseConfigurer);

                  final int keepAliveTimeout = this.keepAliveTimeout;
                  final int keepAliveTickPeriod = this.keepAliveTickPeriod;

                  ErrorFrameMapper errorFrameMapper =
                      errorMappers.createErrorFrameMapper(allocator);

                  RSocket rSocketRequester =
                      rSocketsFactory.createRequester(
                          allocator,
                          multiplexer.asClientConnection(),
                          payloadDecoder,
                          errorConsumer,
                          errorFrameMapper,
                          StreamIdSupplier.clientSupplier(),
                          keepAliveTickPeriod,
                          keepAliveTimeout,
                          keepAliveHandler);

                  if (multiSubscriberRequester) {
                    rSocketRequester = new MultiSubscriberRSocket(rSocketRequester);
                  }

                  RSocket wrappedRSocketRequester = plugins.applyRequester(rSocketRequester);

                  return plugins
                      .applySocketAcceptorInterceptor(acceptor)
                      .accept(connectionSetupPayload, wrappedRSocketRequester)
                      .flatMap(
                          rSocketHandler -> {
                            RSocket wrappedRSocketHandler = plugins.applyResponder(rSocketHandler);

                            RSocket rSocketResponder =
                                rSocketsFactory.createResponder(
                                    allocator,
                                    multiplexer.asServerConnection(),
                                    wrappedRSocketHandler,
                                    payloadDecoder,
                                    errorConsumer,
                                    errorFrameMapper);

                            return wrappedConnection
                                .sendOne(setupFrame)
                                .thenReturn(wrappedRSocketRequester);
                          });
                });
      }

      private int keepAliveTickPeriod() {
        long interval = tickPeriod.toMillis();
        if (interval > Integer.MAX_VALUE) {
          throw new IllegalArgumentException(
              String.format("keep-alive interval millis exceeds INTEGER.MAX_VALUE: %d", interval));
        }

        int minInterval = KEEPALIVE_MIN_INTERVAL_MILLIS;
        if (interval < minInterval) {
          throw new IllegalArgumentException(
              String.format(
                  "keep-alive interval millis is less than minimum of %d: %d",
                  minInterval, interval));
        }

        return (int) interval;
      }

      private int keepAliveTimeout() {
        long timeout = ackTimeout.toMillis();
        if (timeout > Integer.MAX_VALUE) {
          throw new IllegalArgumentException(
              String.format("keep-alive timeout millis exceeds INTEGER.MAX_VALUE: %d", timeout));
        }
        return (int) timeout;
      }

      private ClientSetup clientSetup(DuplexConnection startConnection) {
        if (resumeEnabled) {
          ByteBuf resumeToken = resumeTokenSupplier.get();
          return new ResumableClientSetup(
              allocator,
              startConnection,
              newConnection(),
              resumeToken,
              resumeStoreFactory.apply(resumeToken),
              resumeSessionDuration,
              resumeStreamTimeout,
              resumeStrategySupplier,
              resumeCleanupStoreOnKeepAlive);
        } else {
          return new DefaultClientSetup(startConnection);
        }
      }

      private Mono<DuplexConnection> newConnection() {
        return transportClient.get().connect(frameSizeLimit);
      }
    }
  }

  public static class ServerRSocketFactory {
    private static final String SERVER_TAG = "server";

    private SocketAcceptor acceptor;
    private PayloadDecoder payloadDecoder = PayloadDecoder.ZERO_COPY;
    private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private StreamErrorMappers errorMappers = StreamErrorMappers.create();
    private PluginRegistry plugins = new PluginRegistry(Plugins.defaultPlugins());

    private boolean resumeSupported;
    private Duration resumeSessionDuration = Duration.ofSeconds(120);
    private Duration resumeStreamTimeout = Duration.ofSeconds(10);
    private Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory =
        token -> new InMemoryResumableFramesStore(SERVER_TAG, 100_000);

    private boolean multiSubscriberRequester = true;
    private boolean leaseEnabled;
    private Leases.ServerConfigurer leaseConfigurer = scheduler -> Leases.create();

    private ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private boolean resumeCleanupStoreOnKeepAlive;
    private boolean acceptFragmentedFrames;
    private int frameSizeLimit = FrameLengthFlyweight.FRAME_LENGTH_MASK;

    private ServerRSocketFactory() {}

    public ServerRSocketFactory byteBufAllocator(ByteBufAllocator allocator) {
      Objects.requireNonNull(allocator);
      this.allocator = allocator;
      return this;
    }

    public ServerRSocketFactory addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
      plugins.addConnectionPlugin(interceptor);
      return this;
    }

    public ServerRSocketFactory addRequesterPlugin(RSocketInterceptor interceptor) {
      plugins.addRequesterPlugin(interceptor);
      return this;
    }

    public ServerRSocketFactory addResponderPlugin(RSocketInterceptor interceptor) {
      plugins.addResponderPlugin(interceptor);
      return this;
    }

    public ServerRSocketFactory addSocketAcceptorPlugin(SocketAcceptorInterceptor interceptor) {
      plugins.addSocketAcceptorPlugin(interceptor);
      return this;
    }

    public ServerTransportAcceptor acceptor(SocketAcceptor acceptor) {
      this.acceptor = acceptor;
      return ServerStart::new;
    }

    public ServerRSocketFactory frameDecoder(PayloadDecoder payloadDecoder) {
      this.payloadDecoder = payloadDecoder;
      return this;
    }

    public ServerRSocketFactory acceptFragmentedFrames() {
      this.acceptFragmentedFrames = true;
      return this;
    }

    public ServerRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer) {
      this.errorConsumer = errorConsumer;
      return this;
    }

    /**
     * @param errorMappers configures custom error mappers of incoming and outgoing streams.
     * @return this {@link ServerRSocketFactory} instance
     */
    public ServerRSocketFactory streamErrorMapper(StreamErrorMappers errorMappers) {
      this.errorMappers = errorMappers;
      return this;
    }

    /**
     * Enable and configure requests lease support
     *
     * @param leaseConfigurer configures requests lease
     * @return this {@link ServerRSocketFactory} instance
     */
    public ServerRSocketFactory lease(Leases.ServerConfigurer leaseConfigurer) {
      this.leaseEnabled = true;
      this.leaseConfigurer = Objects.requireNonNull(leaseConfigurer, "leaseConfigurer");
      return this;
    }

    /**
     * Enables requests lease support. Responder will not accept any requests because {@link Leases}
     * is not configured
     *
     * @return this {@link ServerRSocketFactory} instance
     */
    public ServerRSocketFactory lease() {
      this.leaseEnabled = true;
      return this;
    }

    /**
     * Enables requests lease support. Responder will not accept any requests because {@link Leases}
     * is not configured
     *
     * @return this {@link ServerRSocketFactory} instance
     */
    public ServerRSocketFactory lease(boolean leaseEnabled) {
      this.leaseEnabled = leaseEnabled;
      return this;
    }

    public ServerRSocketFactory singleSubscriberRequester() {
      this.multiSubscriberRequester = false;
      return this;
    }

    public ServerRSocketFactory resume() {
      this.resumeSupported = true;
      return this;
    }

    public ServerRSocketFactory resume(boolean resumeEnabled) {
      this.resumeSupported = resumeEnabled;
      return this;
    }

    public ServerRSocketFactory resumeStore(
        Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory) {
      this.resumeStoreFactory = resumeStoreFactory;
      return this;
    }

    public ServerRSocketFactory resumeSessionDuration(Duration sessionDuration) {
      this.resumeSessionDuration = Objects.requireNonNull(sessionDuration);
      return this;
    }

    public ServerRSocketFactory resumeStreamTimeout(Duration resumeStreamTimeout) {
      this.resumeStreamTimeout = Objects.requireNonNull(resumeStreamTimeout);
      return this;
    }

    public ServerRSocketFactory resumeCleanupOnKeepAlive() {
      resumeCleanupStoreOnKeepAlive = true;
      return this;
    }

    public ServerRSocketFactory frameSizeLimit(int frameSizeLimit) {
      this.frameSizeLimit = Preconditions.requireFrameSizeValid(frameSizeLimit);
      return this;
    }

    private class ServerStart<T extends Closeable> implements Start<T> {
      private final Supplier<ServerTransport<T>> transportServer;

      public ServerStart(Supplier<ServerTransport<T>> transportServer) {
        this.transportServer = transportServer;
      }

      @Override
      public Mono<T> start() {
        return Mono.defer(
            new Supplier<Mono<T>>() {

              ServerSetup serverSetup = serverSetup();

              @Override
              public Mono<T> get() {
                return transportServer
                    .get()
                    .start(
                        duplexConnection -> {
                          if (acceptFragmentedFrames) {
                            duplexConnection =
                                new FragmentationDuplexConnection(
                                    duplexConnection, allocator, frameSizeLimit);
                          }

                          ClientServerInputMultiplexer multiplexer =
                              new ClientServerInputMultiplexer(duplexConnection, plugins, false);

                          return multiplexer
                              .asSetupConnection()
                              .receive()
                              .next()
                              .flatMap(startFrame -> accept(startFrame, multiplexer));
                        },
                        frameSizeLimit)
                    .doOnNext(c -> c.onClose().doFinally(v -> serverSetup.dispose()).subscribe());
              }

              private Mono<Void> accept(
                  ByteBuf startFrame, ClientServerInputMultiplexer multiplexer) {
                switch (FrameHeaderFlyweight.frameType(startFrame)) {
                  case SETUP:
                    return acceptSetup(serverSetup, startFrame, multiplexer);
                  case RESUME:
                    return acceptResume(serverSetup, startFrame, multiplexer);
                  default:
                    return acceptUnknown(startFrame, multiplexer);
                }
              }

              private Mono<Void> acceptSetup(
                  ServerSetup serverSetup,
                  ByteBuf setupFrame,
                  ClientServerInputMultiplexer multiplexer) {

                if (!SetupFrameFlyweight.isSupportedVersion(setupFrame)) {
                  return sendError(
                          multiplexer,
                          new InvalidSetupException(
                              "Unsupported version: "
                                  + SetupFrameFlyweight.humanReadableVersion(setupFrame)))
                      .doFinally(
                          signalType -> {
                            setupFrame.release();
                            multiplexer.dispose();
                          });
                }

                boolean isLeaseEnabled = leaseEnabled;

                if (SetupFrameFlyweight.honorLease(setupFrame) && !isLeaseEnabled) {
                  return sendError(multiplexer, new InvalidSetupException("lease is not supported"))
                      .doFinally(
                          signalType -> {
                            setupFrame.release();
                            multiplexer.dispose();
                          });
                }

                return serverSetup.acceptRSocketSetup(
                    setupFrame,
                    multiplexer,
                    (keepAliveHandler, wrappedMultiplexer) -> {
                      ConnectionSetupPayload setupPayload =
                          ConnectionSetupPayload.create(setupFrame);

                      DuplexConnection serverConnection = wrappedMultiplexer.asServerConnection();

                      RSocketsFactory rSocketsFactory =
                          RSocketsFactory.createServer(
                              setupPayload.willClientHonorLease(),
                              serverConnection.scheduler(),
                              leaseConfigurer);

                      ErrorFrameMapper errorFrameMapper =
                          errorMappers.createErrorFrameMapper(allocator);

                      RSocket rSocketRequester =
                          rSocketsFactory.createRequester(
                              allocator,
                              serverConnection,
                              payloadDecoder,
                              errorConsumer,
                              errorFrameMapper,
                              StreamIdSupplier.serverSupplier(),
                              0,
                              setupPayload.keepAliveMaxLifetime(),
                              keepAliveHandler);

                      if (multiSubscriberRequester) {
                        rSocketRequester = new MultiSubscriberRSocket(rSocketRequester);
                      }
                      RSocket wrappedRSocketRequester = plugins.applyRequester(rSocketRequester);

                      return plugins
                          .applySocketAcceptorInterceptor(acceptor)
                          .accept(setupPayload, wrappedRSocketRequester)
                          .onErrorResume(
                              err ->
                                  sendError(multiplexer, rejectedSetupError(err))
                                      .then(Mono.error(err)))
                          .doOnNext(
                              rSocketHandler -> {
                                RSocket wrappedRSocketHandler =
                                    plugins.applyResponder(rSocketHandler);

                                RSocket rSocketResponder =
                                    rSocketsFactory.createResponder(
                                        allocator,
                                        wrappedMultiplexer.asClientConnection(),
                                        wrappedRSocketHandler,
                                        payloadDecoder,
                                        errorConsumer,
                                        errorFrameMapper);
                              })
                          .doFinally(signalType -> setupPayload.release())
                          .then();
                    });
              }

              private Mono<Void> acceptResume(
                  ServerSetup serverSetup,
                  ByteBuf resumeFrame,
                  ClientServerInputMultiplexer multiplexer) {
                return serverSetup.acceptRSocketResume(resumeFrame, multiplexer);
              }
            });
      }

      private ServerSetup serverSetup() {
        return resumeSupported
            ? new ServerSetup.ResumableServerSetup(
                allocator,
                new SessionManager(),
                resumeSessionDuration,
                resumeStreamTimeout,
                resumeStoreFactory,
                resumeCleanupStoreOnKeepAlive)
            : new ServerSetup.DefaultServerSetup(allocator);
      }

      private Mono<Void> acceptUnknown(ByteBuf frame, ClientServerInputMultiplexer multiplexer) {
        return sendError(
                multiplexer,
                new InvalidSetupException(
                    "invalid setup frame: " + FrameHeaderFlyweight.frameType(frame)))
            .doFinally(
                signalType -> {
                  frame.release();
                  multiplexer.dispose();
                });
      }

      private Mono<Void> sendError(ClientServerInputMultiplexer multiplexer, Exception exception) {
        return multiplexer
            .asSetupConnection()
            .sendOne(ErrorFrameFlyweight.encode(allocator, 0, exception))
            .onErrorResume(err -> Mono.empty());
      }

      private Exception rejectedSetupError(Throwable err) {
        String msg = err.getMessage();
        return new RejectedSetupException(msg == null ? "rejected by server acceptor" : msg);
      }
    }
  }
}
