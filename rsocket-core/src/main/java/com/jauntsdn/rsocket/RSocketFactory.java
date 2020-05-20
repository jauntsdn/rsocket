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

import static com.jauntsdn.rsocket.RSocketErrorMappers.*;
import static com.jauntsdn.rsocket.StreamErrorMappers.*;
import static com.jauntsdn.rsocket.internal.ClientSetup.DefaultClientSetup;
import static com.jauntsdn.rsocket.internal.ClientSetup.ResumableClientSetup;

import com.jauntsdn.rsocket.fragmentation.FragmentationDuplexConnection;
import com.jauntsdn.rsocket.frame.*;
import com.jauntsdn.rsocket.frame.decoder.PayloadDecoder;
import com.jauntsdn.rsocket.internal.ClientServerInputMultiplexer;
import com.jauntsdn.rsocket.internal.ClientSetup;
import com.jauntsdn.rsocket.internal.ServerSetup;
import com.jauntsdn.rsocket.keepalive.KeepAliveHandler;
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
import reactor.core.scheduler.Scheduler;

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

    private ClientSocketAcceptor acceptor = (setup, sendingSocket) -> new AbstractRSocket() {};

    private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private StreamErrorMappers streamErrorMappers = StreamErrorMappers.create();
    private RSocketErrorMappers rSocketErrorMappers = RSocketErrorMappers.create();
    private Interceptors.Configurer interceptorsConfigurer = setupPayload -> Interceptors.noop();

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
    private final ClientGracefulDispose gracefulDispose =
        ClientGracefulDispose.create().drainTimeout(Duration.ofSeconds(600));
    private ClientGracefulDispose.Configurer gracefulDisposeConfigurer = gracefulDispose -> {};

    public ClientRSocketFactory byteBufAllocator(ByteBufAllocator allocator) {
      Objects.requireNonNull(allocator);
      this.allocator = allocator;
      return this;
    }

    /**
     * Adds interceptors that are called in order: connection, RSocket requester, socket acceptor,
     * RSocket handler. Global interceptors are called first
     *
     * @param interceptorsConfigurer adds interceptors of connection, RSocket requester, client
     *     socket acceptor, RSocket handler
     * @return this {@link ClientRSocketFactory} instance
     */
    public ClientRSocketFactory interceptors(Interceptors.Configurer interceptorsConfigurer) {
      Objects.requireNonNull(interceptorsConfigurer);
      this.interceptorsConfigurer = interceptorsConfigurer;
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

    public ClientTransportAcceptor acceptor(ClientSocketAcceptor acceptor) {
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
     * @param streamErrorMappers configures custom error mappers of incoming and outgoing streams.
     * @return this {@link ClientRSocketFactory} instance
     */
    public ClientRSocketFactory streamErrorMapper(StreamErrorMappers streamErrorMappers) {
      this.streamErrorMappers = streamErrorMappers;
      return this;
    }

    /**
     * @param rSocketErrorMappers configures custom error mappers of RSocket.
     * @return this {@link ClientRSocketFactory} instance
     */
    public ClientRSocketFactory rSocketErrorMapper(RSocketErrorMappers rSocketErrorMappers) {
      this.rSocketErrorMappers = rSocketErrorMappers;
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

    /**
     * @param gracefulDisposeConfigurer configures graceful dispose of RSocket requester
     * @return this {@link ClientRSocketFactory} instance
     */
    public ClientRSocketFactory gracefulDispose(
        ClientGracefulDispose.Configurer gracefulDisposeConfigurer) {
      this.gracefulDisposeConfigurer =
          Objects.requireNonNull(gracefulDisposeConfigurer, "gracefulDisposeConfigurer");
      return this;
    }

    private class StartClient implements Start<RSocket> {
      private final Supplier<ClientTransport> transportClient;
      private final int keepAliveTickPeriod;
      private final int keepAliveTimeout;
      private final StreamErrorMapper streamErrorMapper =
          streamErrorMappers.createErrorMapper(allocator);
      private final RSocketErrorMapper rSocketErrorMapper =
          rSocketErrorMappers.createErrorMapper(allocator);

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
                  ByteBufAllocator allocator = ClientRSocketFactory.this.allocator;
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

                  Scheduler scheduler = connection.scheduler();
                  KeepAliveHandler keepAliveHandler = clientSetup.keepAliveHandler();
                  DuplexConnection clientConnection = clientSetup.connection();

                  Interceptors interceptors = interceptorsConfigurer.configure(scheduler);
                  DuplexConnection wrappedConnection =
                      interceptors.interceptConnection(clientConnection);

                  ClientServerInputMultiplexer multiplexer =
                      new ClientServerInputMultiplexer(wrappedConnection, true);

                  RSocketsFactory rSocketsFactory =
                      RSocketsFactory.createClient(
                          connectionSetupPayload.willClientHonorLease(),
                          scheduler,
                          leaseConfigurer);

                  final int keepAliveTimeout = this.keepAliveTimeout;
                  final int keepAliveTickPeriod = this.keepAliveTickPeriod;

                  ClientGracefulDispose gracefulDispose = ClientRSocketFactory.this.gracefulDispose;
                  gracefulDisposeConfigurer.configure(gracefulDispose);

                  RSocketRequester gracefullyDisposableRequester =
                      rSocketsFactory.createRequester(
                          allocator,
                          multiplexer.asClientConnection(),
                          payloadDecoder,
                          errorConsumer,
                          streamErrorMapper,
                          rSocketErrorMapper,
                          StreamIdSupplier.clientSupplier(),
                          keepAliveTickPeriod,
                          keepAliveTimeout,
                          keepAliveHandler,
                          gracefulDispose.drainTimeout());

                  RSocket rSocketRequester = gracefullyDisposableRequester;

                  if (multiSubscriberRequester) {
                    rSocketRequester = new MultiSubscriberRSocket(rSocketRequester);
                  }

                  RSocket wrappedRSocketRequester =
                      interceptors.interceptRequester(rSocketRequester);

                  RSocket rSocketHandler =
                      interceptors
                          .interceptClientAcceptor(acceptor)
                          .accept(connectionSetupPayload, wrappedRSocketRequester);

                  RSocket wrappedRSocketHandler = interceptors.interceptHandler(rSocketHandler);

                  RSocketResponder rSocketResponder =
                      rSocketsFactory.createResponder(
                          allocator,
                          multiplexer.asServerConnection(),
                          wrappedRSocketHandler,
                          payloadDecoder,
                          errorConsumer,
                          streamErrorMapper);

                  gracefullyDisposableRequester.onGracefulDispose(
                      rSocketResponder::gracefulDispose);

                  return wrappedConnection.sendOne(setupFrame).thenReturn(wrappedRSocketRequester);
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

    private ServerSocketAcceptor acceptor;
    private PayloadDecoder payloadDecoder = PayloadDecoder.ZERO_COPY;
    private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private StreamErrorMappers streamErrorMappers = StreamErrorMappers.create();
    private RSocketErrorMappers rSocketErrorMappers = RSocketErrorMappers.create();

    private Interceptors.Configurer interceptorsConfigurer = setupPayload -> Interceptors.noop();

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
    private final ServerGracefulDispose gracefulDispose =
        ServerGracefulDispose.create().drainTimeout(Duration.ofSeconds(600));
    private ServerGracefulDispose.Configurer gracefulDisposeConfigurer = gracefulDispose -> {};

    private ServerRSocketFactory() {}

    public ServerRSocketFactory byteBufAllocator(ByteBufAllocator allocator) {
      Objects.requireNonNull(allocator);
      this.allocator = allocator;
      return this;
    }

    /**
     * Adds interceptors that are called in order: connection, RSocket requester, socket acceptor,
     * RSocket handler. Global interceptors are called first
     *
     * @param interceptorsConfigurer adds interceptors of connection, RSocket requester, server
     *     socket acceptor, RSocket handler
     * @return this {@link ServerRSocketFactory} instance
     */
    public ServerRSocketFactory interceptors(Interceptors.Configurer interceptorsConfigurer) {
      Objects.requireNonNull(interceptorsConfigurer);
      this.interceptorsConfigurer = interceptorsConfigurer;
      return this;
    }

    public ServerTransportAcceptor acceptor(ServerSocketAcceptor acceptor) {
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
      this.streamErrorMappers = errorMappers;
      return this;
    }

    /**
     * @param rSocketErrorMappers configures custom error mappers of RSocket.
     * @return this {@link ServerRSocketFactory} instance
     */
    public ServerRSocketFactory rSocketErrorMapper(RSocketErrorMappers rSocketErrorMappers) {
      this.rSocketErrorMappers = rSocketErrorMappers;
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

    /**
     * @param gracefulDisposeConfigurer configures graceful dispose of this server
     * @return this {@link ServerRSocketFactory} instance
     */
    public ServerRSocketFactory gracefulDispose(
        ServerGracefulDispose.Configurer gracefulDisposeConfigurer) {
      this.gracefulDisposeConfigurer =
          Objects.requireNonNull(gracefulDisposeConfigurer, "gracefulDisposeConfigurer");
      return this;
    }

    class ServerStart<T extends Closeable> implements Start<T> {
      private final Supplier<ServerTransport<T>> transportServer;
      private final StreamErrorMapper streamErrorMapper =
          streamErrorMappers.createErrorMapper(allocator);
      private final RSocketErrorMapper rSocketErrorMapper =
          rSocketErrorMappers.createErrorMapper(allocator);

      public ServerStart(Supplier<ServerTransport<T>> transportServer) {
        ServerGracefulDispose gracefulDispose = ServerRSocketFactory.this.gracefulDispose;
        gracefulDisposeConfigurer.configure(gracefulDispose);
        this.transportServer = transportServer;
      }

      @Override
      public Mono<T> start() {
        return Mono.defer(
            new Supplier<Mono<T>>() {
              final ServerSetup serverSetup = serverSetup();
              final ServerGracefulDisposer<T> serverGracefulDisposer =
                  ServerGracefulDisposer.create(gracefulDispose);

              @Override
              public Mono<T> get() {
                serverGracefulDisposer.start();

                return transportServer
                    .get()
                    .start(
                        duplexConnection -> {
                          if (acceptFragmentedFrames) {
                            duplexConnection =
                                new FragmentationDuplexConnection(
                                    duplexConnection, allocator, frameSizeLimit);
                          }
                          Interceptors interceptors =
                              interceptorsConfigurer.configure(duplexConnection.scheduler());

                          DuplexConnection wrappedConnection =
                              interceptors.interceptConnection(duplexConnection);

                          ClientServerInputMultiplexer multiplexer =
                              new ClientServerInputMultiplexer(wrappedConnection, false);

                          return multiplexer
                              .asSetupConnection()
                              .receive()
                              .next()
                              .flatMap(
                                  startFrame -> {
                                    serverGracefulDisposer.beforeAccept();
                                    Mono<Void> a = accept(startFrame, multiplexer, interceptors);
                                    serverGracefulDisposer.afterAccept();
                                    return a;
                                  });
                        },
                        frameSizeLimit)
                    .doOnNext(
                        c -> {
                          serverGracefulDisposer
                              .ofServer(c)
                              .onClose()
                              .doFinally(v -> serverSetup.dispose())
                              .subscribe();
                        });
              }

              private Mono<Void> accept(
                  ByteBuf startFrame,
                  ClientServerInputMultiplexer multiplexer,
                  Interceptors interceptors) {
                switch (FrameHeaderFlyweight.frameType(startFrame)) {
                  case SETUP:
                    return acceptSetup(serverSetup, startFrame, multiplexer, interceptors);
                  case RESUME:
                    return acceptResume(serverSetup, startFrame, multiplexer);
                  default:
                    return acceptUnknown(startFrame, multiplexer);
                }
              }

              private Mono<Void> acceptSetup(
                  ServerSetup serverSetup,
                  ByteBuf setupFrame,
                  ClientServerInputMultiplexer multiplexer,
                  Interceptors interceptors) {
                String reject = serverGracefulDisposer.tryReject();
                if (reject != null) {
                  return sendError(multiplexer, ErrorCodes.CONNECTION_CLOSE, reject)
                      .doFinally(
                          signalType -> {
                            setupFrame.release();
                            multiplexer.dispose();
                          });
                }

                if (!SetupFrameFlyweight.isSupportedVersion(setupFrame)) {
                  return sendError(
                          multiplexer,
                          ErrorCodes.INVALID_SETUP,
                          "Unsupported version: "
                              + SetupFrameFlyweight.humanReadableVersion(setupFrame))
                      .doFinally(
                          signalType -> {
                            setupFrame.release();
                            multiplexer.dispose();
                          });
                }

                boolean isLeaseEnabled = leaseEnabled;

                if (SetupFrameFlyweight.honorLease(setupFrame) && !isLeaseEnabled) {
                  return sendError(multiplexer, ErrorCodes.INVALID_SETUP, "lease is not supported")
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

                      RSocketRequester gracefullyDisposableRequester =
                          rSocketsFactory.createRequester(
                              allocator,
                              serverConnection,
                              payloadDecoder,
                              errorConsumer,
                              streamErrorMapper,
                              rSocketErrorMapper,
                              StreamIdSupplier.serverSupplier(),
                              0,
                              setupPayload.keepAliveMaxLifetime(),
                              keepAliveHandler,
                              gracefulDispose.drainTimeout());

                      RSocket rSocketRequester = gracefullyDisposableRequester;
                      if (multiSubscriberRequester) {
                        rSocketRequester = new MultiSubscriberRSocket(rSocketRequester);
                      }
                      RSocket wrappedRSocketRequester =
                          interceptors.interceptRequester(rSocketRequester);

                      return interceptors
                          .interceptServerAcceptor(acceptor)
                          .accept(setupPayload, wrappedRSocketRequester)
                          .onErrorResume(
                              err ->
                                  sendError(
                                          multiplexer,
                                          ErrorCodes.REJECTED_SETUP,
                                          rejectedSetupErrorMessage(err))
                                      .then(Mono.error(err)))
                          .doOnNext(
                              rSocketHandler -> {
                                RSocket wrappedRSocketHandler =
                                    interceptors.interceptHandler(rSocketHandler);

                                RSocketResponder rSocketResponder =
                                    rSocketsFactory.createResponder(
                                        allocator,
                                        wrappedMultiplexer.asClientConnection(),
                                        wrappedRSocketHandler,
                                        payloadDecoder,
                                        errorConsumer,
                                        streamErrorMapper);

                                gracefullyDisposableRequester.onGracefulDispose(
                                    rSocketResponder::gracefulDispose);

                                serverGracefulDisposer.onConnect(gracefullyDisposableRequester);
                              })
                          .doFinally(signalType -> setupPayload.release())
                          .then();
                    });
              }

              private Mono<Void> acceptResume(
                  ServerSetup serverSetup,
                  ByteBuf resumeFrame,
                  ClientServerInputMultiplexer multiplexer) {

                String reject = serverGracefulDisposer.tryReject();
                if (reject != null) {
                  return sendError(multiplexer, ErrorCodes.REJECTED_RESUME, reject)
                      .doFinally(
                          signalType -> {
                            resumeFrame.release();
                            multiplexer.dispose();
                          });
                }
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
                ErrorCodes.INVALID_SETUP,
                "invalid setup frame: " + FrameHeaderFlyweight.frameType(frame))
            .doFinally(
                signalType -> {
                  frame.release();
                  multiplexer.dispose();
                });
      }

      Mono<Void> sendError(
          ClientServerInputMultiplexer multiplexer, int errorCode, String errorMessage) {
        return multiplexer
            .asSetupConnection()
            .sendOne(
                ErrorFrameFlyweight.encode(
                    allocator, 0, errorCode, rSocketErrorMapper.sendError(errorCode, errorMessage)))
            .onErrorResume(err -> Mono.empty());
      }

      private String rejectedSetupErrorMessage(Throwable err) {
        String msg = err.getMessage();
        return msg == null ? "rejected by server acceptor" : msg;
      }
    }
  }
}
