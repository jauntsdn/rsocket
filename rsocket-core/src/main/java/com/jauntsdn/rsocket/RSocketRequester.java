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
import static com.jauntsdn.rsocket.keepalive.KeepAlive.ClientKeepAlive;

import com.jauntsdn.rsocket.exceptions.ConnectionCloseException;
import com.jauntsdn.rsocket.exceptions.ConnectionErrorException;
import com.jauntsdn.rsocket.frame.*;
import com.jauntsdn.rsocket.frame.decoder.PayloadDecoder;
import com.jauntsdn.rsocket.internal.StreamMonoReceiver;
import com.jauntsdn.rsocket.internal.StreamReceiver;
import com.jauntsdn.rsocket.internal.SynchronizedIntObjectHashMap;
import com.jauntsdn.rsocket.internal.UnboundedProcessor;
import com.jauntsdn.rsocket.keepalive.KeepAlive;
import com.jauntsdn.rsocket.keepalive.KeepAlive.ServerKeepAlive;
import com.jauntsdn.rsocket.keepalive.KeepAliveHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectMap;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import javax.annotation.Nonnull;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.*;
import reactor.core.scheduler.Scheduler;

/**
 * Requester Side of a RSocket socket. Sends {@link ByteBuf}s to a {@link RSocketResponder} of peer
 */
class RSocketRequester implements RSocket {
  private static final Logger logger = LoggerFactory.getLogger(RSocketRequester.class);
  private static final ClosedChannelException CLOSED_CHANNEL_EXCEPTION =
      new ClosedChannelException();
  private static final String GRACEFUL_DISPOSE_MESSAGE = "graceful dispose";

  static {
    CLOSED_CHANNEL_EXCEPTION.setStackTrace(new StackTraceElement[0]);
  }

  private final DuplexConnection connection;
  private final PayloadDecoder payloadDecoder;
  private final Consumer<Throwable> errorConsumer;
  private final StreamErrorMapper streamErrorMapper;
  private final RSocketErrorMapper rSocketErrorMapper;
  private final StreamIdSupplier streamIdSupplier;
  private final Duration gracefulDisposeTimeout;
  private final IntObjectMap<Subscription> senders;
  private final IntObjectMap<Processor<Payload, Payload>> receivers;
  private final UnboundedProcessor<ByteBuf> sendProcessor;
  private final ByteBufAllocator allocator;
  private final Consumer<ByteBuf> keepAliveFramesAcceptor;
  private final Scheduler transportScheduler;
  private final KeepAlive keepAlive;
  private final Disposable onReceiveOverflow;

  private volatile Throwable terminationError;
  private volatile boolean isGracefulDisposedRemote;
  private String gracefulDisposeRemoteMessage;
  private volatile boolean isGracefulDisposedLocal;
  private Consumer<String> onGracefulDisposeLocal;
  private Disposable gracefulDisposeLocalTimeout = Disposables.disposed();

  RSocketRequester(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer,
      StreamErrorMapper streamErrorMapper,
      RSocketErrorMapper rSocketErrorMapper,
      StreamIdSupplier streamIdSupplier,
      int keepAliveTickPeriod,
      int keepAliveAckTimeout,
      KeepAliveHandler keepAliveHandler,
      Duration gracefulDisposeTimeout,
      boolean validate) {
    this.allocator = allocator;
    this.connection = connection;
    this.payloadDecoder = payloadDecoder;
    this.errorConsumer = errorConsumer;
    this.streamErrorMapper = streamErrorMapper;
    this.rSocketErrorMapper = rSocketErrorMapper;
    this.streamIdSupplier = streamIdSupplier;
    this.gracefulDisposeTimeout = gracefulDisposeTimeout;
    this.senders = new SynchronizedIntObjectHashMap<>();
    this.receivers = new SynchronizedIntObjectHashMap<>();
    this.transportScheduler = connection.scheduler();
    this.onReceiveOverflow = onReceiveOverflow(validate);
    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    this.sendProcessor = new UnboundedProcessor<>();

    connection
        .onClose()
        .doFinally(signalType -> handleConnectionClose())
        .subscribe(null, errorConsumer);

    connection.send(sendProcessor).subscribe(null, this::handleSendProcessorError);

    connection.receive().subscribe(this::handleIncomingFrames, errorConsumer);

    KeepAlive keepAlive =
        keepAliveTickPeriod != 0
            ? new ClientKeepAlive(
                transportScheduler,
                allocator,
                keepAliveTickPeriod,
                keepAliveAckTimeout,
                keepAliveFrame -> sendProcessor.onNext(keepAliveFrame))
            : new ServerKeepAlive(
                transportScheduler,
                allocator,
                keepAliveAckTimeout,
                keepAliveFrame -> sendProcessor.onNext(keepAliveFrame));
    this.keepAlive = keepAlive;
    this.keepAliveFramesAcceptor =
        keepAliveHandler.start(
            keepAlive, () -> tryTerminate(TerminationSignal.keepAliveTimeout(keepAliveAckTimeout)));
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return handleFireAndForget(payload);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return handleRequestResponse(payload);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return handleRequestStream(payload);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return handleChannel(Flux.from(payloads));
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return handleMetadataPush(payload);
  }

  @Override
  public double availability() {
    double rSocketAvailability = isGracefulDisposedRemote ? 0.0 : 1.0;
    return Math.min(rSocketAvailability, connection.availability());
  }

  @Override
  public Optional<Scheduler> scheduler() {
    return Optional.of(transportScheduler);
  }

  @Override
  public void dispose() {
    connection.dispose();
  }

  @Override
  public void dispose(String errorMessage, boolean isGraceful) {
    if (terminationError == null) {
      transportScheduler.schedule(
          () -> {
            if (isGraceful) {
              if (!isGracefulDisposedLocal) {
                isGracefulDisposedLocal = true;
                String msg = errorMessage.isEmpty() ? GRACEFUL_DISPOSE_MESSAGE : errorMessage;
                gracefulDispose(msg);
                onGracefulDisposeLocal.accept(msg);
              }
            } else {
              tryTerminate(TerminationSignal.errorLocal(errorMessage));
            }
          });
    }
  }

  @Override
  public boolean isDisposed() {
    return connection.isDisposed() || isGracefulDisposedRemote;
  }

  @Override
  public Mono<Void> onClose() {
    return connection.onClose();
  }

  Throwable checkAllowed() {
    return terminationError;
  }

  KeepAlive keepAlive() {
    return keepAlive;
  }

  private Mono<Void> handleFireAndForget(Payload payload) {
    final Throwable err = checkAllowed();
    if (err != null) {
      payload.release();
      return Mono.error(err);
    }

    return Mono.create(
            new Consumer<MonoSink<Void>>() {

              boolean isRequestSent;

              @Override
              public void accept(MonoSink<Void> sink) {
                if (isRequestSent) {
                  sink.error(new IllegalStateException("only a single Subscriber is allowed"));
                  return;
                }
                isRequestSent = true;

                Throwable e = terminationError;
                if (e != null) {
                  sink.error(e);
                  payload.release();
                  return;
                }

                final int streamId = streamIdSupplier.nextStreamId(receivers);
                final ByteBuf requestFrame =
                    RequestFireAndForgetFrameFlyweight.encode(
                        allocator,
                        streamId,
                        payload.hasMetadata() ? payload.sliceMetadata().retain() : null,
                        payload.sliceData().retain());
                payload.release();

                sendProcessor.onNext(requestFrame);
                sink.success();
              }
            })
        .subscribeOn(transportScheduler);
  }

  private Mono<Payload> handleRequestResponse(final Payload payload) {
    final Throwable err = checkAllowed();
    if (err != null) {
      payload.release();
      return Mono.error(err);
    }
    final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;

    final StreamMonoReceiver receiver = StreamMonoReceiver.create();
    return receiver
        .doOnSubscribe(
            new Consumer<Subscription>() {

              boolean isRequestSent;

              @Override
              public void accept(@Nonnull Subscription subscription) {
                if (isRequestSent) {
                  receiver.onError(
                      new IllegalStateException("only a single Subscriber is allowed"));
                  return;
                }
                isRequestSent = true;

                if (receiver.isTerminated()) {
                  payload.release();
                  return;
                }

                Throwable e = terminationError;
                if (e != null) {
                  receiver.onError(e);
                  payload.release();
                  return;
                }

                int streamId = receiver.streamId(streamIdSupplier.nextStreamId(receivers));
                receivers.put(streamId, receiver);

                final ByteBuf requestFrame =
                    RequestResponseFrameFlyweight.encode(
                        allocator,
                        streamId,
                        payload.hasMetadata() ? payload.sliceMetadata().retain() : null,
                        payload.sliceData().retain());
                payload.release();

                sendProcessor.onNext(requestFrame);
              }
            })
        .doFinally(
            signalType -> {
              int streamId = receiver.streamId();
              if (signalType == SignalType.CANCEL && !receiver.isTerminated()) {
                sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId));
              }
              removeReceiver(streamId);
            })
        .subscribeOn(transportScheduler);
  }

  private Flux<Payload> handleRequestStream(final Payload payload) {
    final Throwable err = checkAllowed();
    if (err != null) {
      payload.release();
      return Flux.error(err);
    }

    final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;
    final StreamReceiver receiver = StreamReceiver.create(onReceiveOverflow);

    return receiver
        .doOnRequest(
            new LongConsumer() {

              boolean isRequestSent;

              @Override
              public void accept(long requestN) {

                if (receiver.isDisposed()) {
                  if (!isRequestSent) {
                    payload.release();
                  }
                  return;
                }

                Throwable e = terminationError;
                if (e != null) {
                  receiver.onError(e);
                  if (!isRequestSent) {
                    payload.release();
                  }
                  return;
                }

                if (!isRequestSent) {
                  isRequestSent = true;

                  final int streamId = receiver.streamId(streamIdSupplier.nextStreamId(receivers));
                  receivers.put(streamId, receiver);

                  sendProcessor.onNext(
                      RequestStreamFrameFlyweight.encode(
                          allocator,
                          streamId,
                          requestN,
                          payload.hasMetadata() ? payload.sliceMetadata().retain() : null,
                          payload.sliceData().retain()));
                  payload.release();
                } else {
                  final int streamId = receiver.streamId();
                  sendProcessor.onNext(
                      RequestNFrameFlyweight.encode(allocator, streamId, requestN));
                }
              }
            })
        .doFinally(
            signalType -> {
              final int streamId = receiver.streamId();
              if (streamId < 0) {
                return;
              }
              if (signalType == SignalType.CANCEL && !receiver.isTerminated()) {
                sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId));
              }
              removeReceiver(streamId);
            })
        .subscribeOn(transportScheduler);
  }

  private Flux<Payload> handleChannel(Flux<Payload> request) {
    Throwable err = checkAllowed();
    if (err != null) {
      return Flux.error(err);
    }

    final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;
    final StreamReceiver receiver = StreamReceiver.create(onReceiveOverflow);

    return receiver
        .doOnRequest(
            new LongConsumer() {

              boolean isRequestSent;

              @Override
              public void accept(long requestN) {
                if (receiver.isDisposed()) {
                  return;
                }
                if (!isRequestSent) {
                  isRequestSent = true;
                  request
                      .publishOn(transportScheduler)
                      .subscribe(
                          new BaseSubscriber<Payload>() {

                            boolean isRequestSent;
                            Subscription sender;

                            @Override
                            protected void hookOnSubscribe(Subscription subscription) {
                              this.sender = subscription;
                              receiver.streamSender(subscription);
                              subscription.request(1);
                            }

                            @Override
                            protected void hookOnNext(Payload payload) {
                              final ByteBuf frame;

                              if (receiver.isDisposed()) {
                                payload.release();
                                return;
                              }

                              Throwable e = terminationError;
                              if (e != null) {
                                receiver.onError(e);
                                payload.release();
                                return;
                              }

                              if (!isRequestSent) {
                                isRequestSent = true;

                                int streamId =
                                    receiver.streamId(streamIdSupplier.nextStreamId(receivers));
                                senders.put(streamId, sender);
                                receivers.put(streamId, receiver);

                                frame =
                                    RequestChannelFrameFlyweight.encode(
                                        allocator,
                                        streamId,
                                        requestN,
                                        payload.hasMetadata()
                                            ? payload.sliceMetadata().retain()
                                            : null,
                                        payload.sliceData().retain());
                              } else {
                                final int streamId = receiver.streamId();
                                frame =
                                    PayloadFrameFlyweight.encodeNext(allocator, streamId, payload);
                              }

                              sendProcessor.onNext(frame);
                              payload.release();
                            }

                            @Override
                            protected void hookOnComplete() {
                              int streamId = receiver.streamId();
                              if (streamId < 0) {
                                receiver.onComplete();
                              } else if (!receiver.isDisposed()) {
                                sendProcessor.onNext(
                                    PayloadFrameFlyweight.encodeComplete(allocator, streamId));
                              }
                            }

                            @Override
                            protected void hookOnError(Throwable t) {
                              int streamId = receiver.streamId();
                              if (streamId > 0) {
                                sendProcessor.onNext(
                                    streamErrorMapper.streamErrorToFrame(
                                        streamId, StreamType.REQUEST, t));
                              }
                              // todo signal error wrapping request error
                              receiver.dispose();
                            }
                          });
                } else {
                  final int streamId = receiver.streamId();
                  sendProcessor.onNext(
                      RequestNFrameFlyweight.encode(allocator, streamId, requestN));
                }
              }
            })
        .doFinally(
            s -> {
              int streamId = receiver.streamId();
              if (streamId < 0) {
                Subscription sender = receiver.streamSender();
                if (sender != null) {
                  sender.cancel();
                }
                return;
              }
              if (s == SignalType.CANCEL && !receiver.isTerminated()) {
                sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId));
              }
              removeReceiverAndSender(streamId);
            });
  }

  private Mono<Void> handleMetadataPush(Payload payload) {
    Throwable e = this.terminationError;
    if (e != null) {
      payload.release();
      return Mono.error(e);
    }

    return Mono.create(
        new Consumer<MonoSink<Void>>() {

          boolean isRequestSent;

          @Override
          public void accept(MonoSink<Void> sink) {
            if (isRequestSent) {
              sink.error(new IllegalStateException("only a single Subscriber is allowed"));
              return;
            }
            isRequestSent = true;

            Throwable err = terminationError;
            if (err != null) {
              payload.release();
              sink.error(err);
              return;
            }

            ByteBuf metadataPushFrame =
                MetadataPushFrameFlyweight.encode(allocator, payload.sliceMetadata().retain());
            payload.release();

            sendProcessor.onNext(metadataPushFrame);
            sink.success();
          }
        });
  }

  private void handleIncomingFrames(ByteBuf frame) {
    try {
      int streamId = FrameHeaderFlyweight.streamId(frame);
      FrameType type = FrameHeaderFlyweight.strictFrameType(frame);
      if (streamId == 0) {
        handleZeroFrame(type, frame);
      } else {
        handleStreamFrame(streamId, type, frame);
      }
      frame.release();
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(frame);
      throw reactor.core.Exceptions.propagate(t);
    }
  }

  private void handleZeroFrame(FrameType type, ByteBuf frame) {
    switch (type) {
      case ERROR:
        handleZeroErrorFrame(frame);
        break;
      case LEASE:
        handleLeaseFrame(frame);
        break;
      case KEEPALIVE:
        keepAliveFramesAcceptor.accept(frame);
        break;
      default:
        // Ignore unknown frames. Throwing an error will close the socket.
        errorConsumer.accept(
            new IllegalStateException(
                "Client received supported frame on stream 0: " + frame.toString()));
    }
  }

  private void handleZeroErrorFrame(ByteBuf frame) {
    if (ErrorFrameFlyweight.errorCode(frame) == ErrorCodes.CONNECTION_CLOSE) {
      gracefulDisposeRemote(ErrorFrameFlyweight.dataUtf8(frame));
    } else {
      tryTerminate(TerminationSignal.errorRemote(frame));
    }
  }

  void handleLeaseFrame(ByteBuf frame) {}

  private void handleStreamFrame(int streamId, FrameType type, ByteBuf frame) {
    Subscriber<Payload> receiver = receivers.get(streamId);
    if (receiver != null) {
      switch (type) {
        case ERROR:
          receiver.onError(streamErrorMapper.streamFrameToError(frame, StreamType.RESPONSE));
          receivers.remove(streamId);
          break;
        case NEXT_COMPLETE:
          receiver.onNext(payloadDecoder.apply(frame, type));
          receiver.onComplete();
          break;
        case CANCEL:
          {
            Subscription sender = senders.remove(streamId);
            if (sender != null) {
              sender.cancel();
            }
            break;
          }
        case NEXT:
          receiver.onNext(payloadDecoder.apply(frame, type));
          break;
        case REQUEST_N:
          {
            Subscription sender = senders.get(streamId);
            if (sender != null) {
              int n = RequestNFrameFlyweight.requestN(frame);
              sender.request(n >= Integer.MAX_VALUE ? Long.MAX_VALUE : n);
            }
            break;
          }
        case COMPLETE:
          receiver.onComplete();
          receivers.remove(streamId);
          break;
        default:
          throw new IllegalStateException(
              "Client received supported frame on stream " + streamId + ": " + frame.toString());
      }
    }
  }

  private void tryTerminate(TerminationSignal signal) {
    if (terminationError == null) {
      gracefulDisposeLocalTimeout.dispose();

      Exception e;
      switch (signal.type()) {
        case CLOSE:
          e = signal.value();
          break;
        case GRACEFUL_CLOSE_REMOTE:
          e = rSocketErrorMapper.receiveError(ErrorCodes.CONNECTION_CLOSE, signal.value());
          break;
        case GRACEFUL_CLOSE_LOCAL:
          e = new ConnectionCloseException(signal.value());
          break;
        case KEEP_ALIVE_TIMEOUT:
          e =
              new ConnectionErrorException(
                  String.format("No keep-alive acks for %d ms", signal.<Integer>value()));
          break;
        case ERROR_REMOTE:
          e = rSocketErrorMapper.receiveErrorFrame(signal.value());
          break;
        case ERROR_LOCAL:
          String errorMessage = signal.value();
          String msg = rSocketErrorMapper.sendError(ErrorCodes.CONNECTION_ERROR, errorMessage);
          sendProcessor.onNext(
              ErrorFrameFlyweight.encode(allocator, 0, ErrorCodes.CONNECTION_ERROR, msg));
          e = new ConnectionErrorException(errorMessage);
          break;
        default:
          e = new IllegalStateException("Unknown termination signal: " + signal.type());
      }
      this.terminationError = e;
      terminate(e);
    }
  }

  private void terminate(Exception e) {
    connection.dispose();

    synchronized (receivers) {
      receivers
          .values()
          .forEach(
              receiver -> {
                try {
                  receiver.onError(e);
                } catch (Throwable t) {
                  errorConsumer.accept(t);
                }
              });
    }
    synchronized (senders) {
      senders
          .values()
          .forEach(
              sender -> {
                try {
                  sender.cancel();
                } catch (Throwable t) {
                  errorConsumer.accept(t);
                }
              });
    }
    senders.clear();
    receivers.clear();
    sendProcessor.dispose();
    errorConsumer.accept(e);
  }

  private void removeReceiver(int streamId) {
    /*on termination receivers are explicitly cleared to avoid removing from map while iterating over one
    of its views*/
    if (terminationError == null) {
      receivers.remove(streamId);
    }
  }

  private void removeReceiverAndSender(int streamId) {
    /*on termination senders & receivers are explicitly cleared to avoid removing from map while iterating over one
    of its views*/
    if (terminationError == null) {
      receivers.remove(streamId);
      Subscription sender = senders.remove(streamId);
      if (sender != null) {
        sender.cancel();
      }
    }
  }

  private void handleConnectionClose() {
    String msg = this.gracefulDisposeRemoteMessage;
    tryTerminate(
        msg != null
            ? TerminationSignal.gracefulCloseRemote(msg)
            : TerminationSignal.close(CLOSED_CHANNEL_EXCEPTION));
  }

  private void handleSendProcessorError(Throwable t) {
    connection.dispose();
  }

  private Disposable onReceiveOverflow(boolean validate) {
    return validate
        ? () -> dispose("Requester stream received more frames than demanded with requestN", false)
        : null;
  }

  RSocketRequester onGracefulDispose(Consumer<String> onGracefulDispose) {
    this.onGracefulDisposeLocal = onGracefulDispose;
    return this;
  }

  void gracefulDispose(String msg) {
    logger.debug("Local graceful dispose RSocketRequester with message: {}", msg);
    String message = rSocketErrorMapper.sendError(ErrorCodes.CONNECTION_CLOSE, msg);
    sendProcessor.onNext(
        ErrorFrameFlyweight.encode(allocator, 0, ErrorCodes.CONNECTION_CLOSE, message));
    gracefulDisposeLocalTimeout =
        transportScheduler.schedule(
            () -> tryTerminate(TerminationSignal.gracefulCloseLocal(message)),
            gracefulDisposeTimeout.toMillis(),
            TimeUnit.MILLISECONDS);
  }

  void gracefulDisposeRemote(String msg) {
    logger.debug("Remote graceful dispose RSocketRequester with message: {}", msg);
    isGracefulDisposedRemote = true;
    gracefulDisposeRemoteMessage = msg;
  }

  private static class TerminationSignal {
    private final Object value;
    private final Type type;

    private TerminationSignal(Object value, Type type) {
      this.value = value;
      this.type = type;
    }

    public static TerminationSignal keepAliveTimeout(int timeout) {
      return new TerminationSignal(timeout, Type.KEEP_ALIVE_TIMEOUT);
    }

    public static TerminationSignal close(ClosedChannelException e) {
      return new TerminationSignal(e, Type.CLOSE);
    }

    public static TerminationSignal gracefulCloseLocal(String message) {
      return new TerminationSignal(message, Type.GRACEFUL_CLOSE_LOCAL);
    }

    public static TerminationSignal gracefulCloseRemote(String message) {
      return new TerminationSignal(message, Type.GRACEFUL_CLOSE_REMOTE);
    }

    public static TerminationSignal errorRemote(ByteBuf connectionErrorFrame) {
      return new TerminationSignal(connectionErrorFrame, Type.ERROR_REMOTE);
    }

    public static TerminationSignal errorLocal(String errorMessage) {
      return new TerminationSignal(errorMessage, Type.ERROR_LOCAL);
    }

    @SuppressWarnings("unchecked")
    public <T> T value() {
      return (T) value;
    }

    public Type type() {
      return type;
    }

    enum Type {
      CLOSE,
      GRACEFUL_CLOSE_REMOTE,
      GRACEFUL_CLOSE_LOCAL,
      KEEP_ALIVE_TIMEOUT,
      ERROR_REMOTE,
      ERROR_LOCAL,
    }
  }
}
