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

import static com.jauntsdn.rsocket.keepalive.KeepAlive.ClientKeepAlive;

import com.jauntsdn.rsocket.exceptions.ConnectionErrorException;
import com.jauntsdn.rsocket.exceptions.Exceptions;
import com.jauntsdn.rsocket.frame.*;
import com.jauntsdn.rsocket.frame.decoder.PayloadDecoder;
import com.jauntsdn.rsocket.internal.SynchronizedIntObjectHashMap;
import com.jauntsdn.rsocket.internal.UnboundedProcessor;
import com.jauntsdn.rsocket.keepalive.KeepAlive;
import com.jauntsdn.rsocket.keepalive.KeepAlive.ServerKeepAlive;
import com.jauntsdn.rsocket.keepalive.KeepAliveHandler;
import com.jauntsdn.rsocket.lease.RequesterLeaseHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectMap;
import java.nio.channels.ClosedChannelException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import javax.annotation.Nonnull;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.*;
import reactor.core.scheduler.Scheduler;

/**
 * Requester Side of a RSocket socket. Sends {@link ByteBuf}s to a {@link RSocketResponder} of peer
 */
class RSocketRequester implements RSocket {
  private static final AtomicReferenceFieldUpdater<RSocketRequester, Throwable> TERMINATION_ERROR =
      AtomicReferenceFieldUpdater.newUpdater(
          RSocketRequester.class, Throwable.class, "terminationError");
  private static final Exception CLOSED_CHANNEL_EXCEPTION = new ClosedChannelException();

  static {
    CLOSED_CHANNEL_EXCEPTION.setStackTrace(new StackTraceElement[0]);
  }

  private final DuplexConnection connection;
  private final PayloadDecoder payloadDecoder;
  private final Consumer<Throwable> errorConsumer;
  private final StreamIdSupplier streamIdSupplier;
  private final IntObjectMap<Subscription> senders;
  private final IntObjectMap<Processor<Payload, Payload>> receivers;
  private final UnboundedProcessor<ByteBuf> sendProcessor;
  private final RequesterLeaseHandler leaseHandler;
  private final ByteBufAllocator allocator;
  private final Consumer<ByteBuf> keepAliveFramesAcceptor;
  private final Scheduler transportScheduler;
  private volatile Throwable terminationError;

  RSocketRequester(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer,
      StreamIdSupplier streamIdSupplier,
      int keepAliveTickPeriod,
      int keepAliveAckTimeout,
      KeepAliveHandler keepAliveHandler,
      RequesterLeaseHandler leaseHandler) {
    this.allocator = allocator;
    this.connection = connection;
    this.payloadDecoder = payloadDecoder;
    this.errorConsumer = errorConsumer;
    this.streamIdSupplier = streamIdSupplier;
    this.leaseHandler = leaseHandler;
    this.senders = new SynchronizedIntObjectHashMap<>();
    this.receivers = new SynchronizedIntObjectHashMap<>();
    this.transportScheduler = connection.scheduler();

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    this.sendProcessor = new UnboundedProcessor<>();

    connection
        .onClose()
        .doFinally(signalType -> tryTerminate(CLOSED_CHANNEL_EXCEPTION))
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
    this.keepAliveFramesAcceptor =
        keepAliveHandler.start(keepAlive, () -> tryTerminate(keepAliveAckTimeout));
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
    return Math.min(connection.availability(), leaseHandler.availability());
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
  public boolean isDisposed() {
    return connection.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return connection.onClose();
  }

  private Mono<Void> handleFireAndForget(Payload payload) {
    final Throwable err = checkAvailable();
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
    final Throwable err = checkAvailable();
    if (err != null) {
      payload.release();
      return Mono.error(err);
    }
    final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;

    final Stream stream = new Stream();
    final MonoProcessor<Payload> receiver = MonoProcessor.create();
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

                Throwable err = terminationError;
                if (err != null) {
                  receiver.onError(err);
                  payload.release();
                  return;
                }

                int streamId = stream.setId(streamIdSupplier.nextStreamId(receivers));
                receivers.put(streamId, receiver);

                final ByteBuf requestFrame =
                    RequestResponseFrameFlyweight.encode(
                        allocator,
                        streamId,
                        payload.sliceMetadata().retain(),
                        payload.sliceData().retain());
                payload.release();

                sendProcessor.onNext(requestFrame);
              }
            })
        .doFinally(
            signalType -> {
              int streamId = stream.getId();
              if (signalType == SignalType.CANCEL && !receiver.isTerminated()) {
                sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId));
              }
              removeReceiver(streamId);
            })
        .subscribeOn(transportScheduler);
  }

  private Flux<Payload> handleRequestStream(final Payload payload) {
    final Throwable err = checkAvailable();
    if (err != null) {
      payload.release();
      return Flux.error(err);
    }

    final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;
    final UnicastProcessor<Payload> receiver = UnicastProcessor.create();
    final Stream stream = new Stream();

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

                Throwable err = terminationError;
                if (err != null) {
                  receiver.onError(err);
                  if (!isRequestSent) {
                    payload.release();
                  }
                  return;
                }

                if (!isRequestSent) {
                  isRequestSent = true;

                  final int streamId = stream.setId(streamIdSupplier.nextStreamId(receivers));
                  receivers.put(streamId, receiver);

                  sendProcessor.onNext(
                      RequestStreamFrameFlyweight.encode(
                          allocator,
                          streamId,
                          requestN,
                          payload.sliceMetadata().retain(),
                          payload.sliceData().retain()));
                  payload.release();
                } else {
                  final int streamId = stream.getId();
                  sendProcessor.onNext(
                      RequestNFrameFlyweight.encode(allocator, streamId, requestN));
                }
              }
            })
        .doFinally(
            signalType -> {
              final int streamId = stream.getId();
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
    Throwable err = checkAvailable();
    if (err != null) {
      return Flux.error(err);
    }

    final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;
    final UnicastProcessor<Payload> receiver = UnicastProcessor.create();
    final Stream stream = new Stream();

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
                              stream.setSender(subscription);
                              subscription.request(1);
                            }

                            @Override
                            protected void hookOnNext(Payload payload) {
                              final ByteBuf frame;

                              if (receiver.isDisposed()) {
                                payload.release();
                                return;
                              }

                              Throwable err = terminationError;
                              if (err != null) {
                                receiver.onError(err);
                                payload.release();
                                return;
                              }

                              if (!isRequestSent) {
                                isRequestSent = true;

                                int streamId =
                                    stream.setId(streamIdSupplier.nextStreamId(receivers));
                                senders.put(streamId, sender);
                                receivers.put(streamId, receiver);

                                frame =
                                    RequestChannelFrameFlyweight.encode(
                                        allocator,
                                        streamId,
                                        requestN,
                                        payload.sliceMetadata().retain(),
                                        payload.sliceData().retain());
                              } else {
                                final int streamId = stream.getId();
                                frame =
                                    PayloadFrameFlyweight.encodeNext(allocator, streamId, payload);
                              }

                              sendProcessor.onNext(frame);
                              payload.release();
                            }

                            @Override
                            protected void hookOnComplete() {
                              int streamId = stream.getId();
                              if (streamId < 0) {
                                receiver.onComplete();
                              } else if (!receiver.isDisposed()) {
                                sendProcessor.onNext(
                                    PayloadFrameFlyweight.encodeComplete(allocator, streamId));
                              }
                            }

                            @Override
                            protected void hookOnError(Throwable t) {
                              int streamId = stream.getId();
                              if (streamId > 0) {
                                sendProcessor.onNext(
                                    ErrorFrameFlyweight.encode(allocator, streamId, t));
                              }
                              // todo signal error wrapping request error
                              receiver.dispose();
                            }
                          });
                } else {
                  final int streamId = stream.getId();
                  sendProcessor.onNext(
                      RequestNFrameFlyweight.encode(allocator, streamId, requestN));
                }
              }
            })
        .doFinally(
            s -> {
              int streamId = stream.getId();
              if (streamId < 0) {
                Subscription sender = stream.getSender();
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
    Throwable err = this.terminationError;
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

  private Throwable checkAvailable() {
    Throwable err = this.terminationError;
    if (err != null) {
      return err;
    }
    RequesterLeaseHandler lh = leaseHandler;
    if (!lh.useLease()) {
      return lh.leaseError();
    }
    return null;
  }

  private void handleIncomingFrames(ByteBuf frame) {
    try {
      int streamId = FrameHeaderFlyweight.streamId(frame);
      FrameType type = FrameHeaderFlyweight.strictFrameType(frame);
      if (streamId == 0) {
        handleStreamZero(type, frame);
      } else {
        handleFrame(streamId, type, frame);
      }
      frame.release();
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(frame);
      throw reactor.core.Exceptions.propagate(t);
    }
  }

  private void handleStreamZero(FrameType type, ByteBuf frame) {
    switch (type) {
      case ERROR:
        tryTerminate(frame);
        break;
      case LEASE:
        leaseHandler.receive(frame);
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

  private void handleFrame(int streamId, FrameType type, ByteBuf frame) {
    Subscriber<Payload> receiver = receivers.get(streamId);
    if (receiver == null) {
      handleMissingResponseProcessor(streamId, type, frame);
    } else {
      switch (type) {
        case ERROR:
          receiver.onError(Exceptions.from(frame));
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

  private void handleMissingResponseProcessor(int streamId, FrameType type, ByteBuf frame) {
    if (!streamIdSupplier.isBeforeOrCurrent(streamId)) {
      if (type == FrameType.ERROR) {
        // message for stream that has never existed, we have a problem with
        // the overall connection and must tear down
        String errorMessage = ErrorFrameFlyweight.dataUtf8(frame);

        throw new IllegalStateException(
            "Client received error for non-existent stream: "
                + streamId
                + " Message: "
                + errorMessage);
      } else {
        throw new IllegalStateException(
            "Client received message for non-existent stream: "
                + streamId
                + ", frame type: "
                + type);
      }
    }
    // receiving a frame after a given stream has been cancelled/completed,
    // so ignore (cancellation is async so there is a race condition)
  }

  /*error is (int (KeepAlive timeout) | ClosedChannelException (dispose) | Error frame (0 error)) */
  /*no race because executed on transport scheduler */
  private void tryTerminate(Object error) {
    if (terminationError == null) {
      Exception e;
      if (error instanceof ClosedChannelException) {
        e = (Exception) error;
      } else if (error instanceof Integer) {
        Integer keepAliveTimeout = (Integer) error;
        e =
            new ConnectionErrorException(
                String.format("No keep-alive acks for %d ms", keepAliveTimeout));
      } else if (error instanceof ByteBuf) {
        ByteBuf errorFrame = (ByteBuf) error;
        e = Exceptions.from(errorFrame);
      } else {
        e = new IllegalStateException("Unknown termination token: " + error);
      }
      this.terminationError = e;
      terminate(e);
    }
  }

  private void terminate(Exception e) {
    connection.dispose();
    leaseHandler.dispose();

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

  private void handleSendProcessorError(Throwable t) {
    connection.dispose();
  }

  private static class Stream {
    private volatile int id = -1;
    private volatile Subscription sender;

    public int setId(int streamId) {
      this.id = streamId;
      return streamId;
    }

    public int getId() {
      return id;
    }

    public Subscription getSender() {
      return sender;
    }

    public void setSender(Subscription sender) {
      this.sender = sender;
    }
  }
}
