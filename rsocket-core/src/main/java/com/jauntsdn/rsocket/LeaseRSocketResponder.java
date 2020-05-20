package com.jauntsdn.rsocket;

import static com.jauntsdn.rsocket.StreamErrorMappers.*;

import com.jauntsdn.rsocket.frame.FrameType;
import com.jauntsdn.rsocket.frame.decoder.PayloadDecoder;
import com.jauntsdn.rsocket.lease.*;
import io.netty.buffer.ByteBufAllocator;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;

class LeaseRSocketResponder extends RSocketResponder {
  private static final Logger logger = LoggerFactory.getLogger(LeaseRSocketResponder.class);

  private final ResponderLeaseHandler leaseHandler;
  private final Disposable leaseDisposable;

  LeaseRSocketResponder(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer,
      StreamErrorMapper streamErrorMapper,
      ResponderLeaseHandler leaseHandler) {
    super(allocator, connection, requestHandler, payloadDecoder, errorConsumer, streamErrorMapper);
    this.leaseHandler = leaseHandler;
    this.leaseDisposable = leaseHandler.send(this::sendFrame);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    try {
      ResponderLeaseHandler leaseHandler = this.leaseHandler;
      FrameType requestType = FrameType.REQUEST_FNF;
      Signal<?> requestOrReject = leaseHandler.allowRequest(requestType, payload.metadata());
      Throwable reject = requestOrReject.getThrowable();
      if (reject != null) {
        payload.release();
        return Mono.error(reject);
      } else {
        Mono<Void> response = super.fireAndForget(payload);
        if (leaseHandler.requireStats()) {
          MonoSignalStatsRecorder recorder =
              new MonoSignalStatsRecorder(leaseHandler, requestType, requestOrReject.get());
          return response.doOnEach(recorder).doOnCancel(recorder);
        }
        return response;
      }
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      ResponderLeaseHandler leaseHandler = this.leaseHandler;
      FrameType requestType = FrameType.REQUEST_RESPONSE;
      Signal<?> requestOrReject = leaseHandler.allowRequest(requestType, payload.metadata());
      Throwable reject = requestOrReject.getThrowable();
      if (reject != null) {
        payload.release();
        return Mono.error(reject);
      } else {
        Mono<Payload> response = super.requestResponse(payload);
        if (leaseHandler.requireStats()) {
          MonoSignalStatsRecorder recorder =
              new MonoSignalStatsRecorder(leaseHandler, requestType, requestOrReject.get());
          return response.doOnEach(recorder).doOnCancel(recorder);
        }
        return response;
      }

    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    try {
      ResponderLeaseHandler leaseHandler = this.leaseHandler;
      FrameType requestType = FrameType.REQUEST_STREAM;
      Signal<?> requestOrReject = leaseHandler.allowRequest(requestType, payload.metadata());
      Throwable reject = requestOrReject.getThrowable();
      if (reject != null) {
        payload.release();
        return Flux.error(reject);
      } else {
        Flux<Payload> response = super.requestStream(payload);
        if (leaseHandler.requireStats()) {
          FluxSignalStatsRecorder recorder =
              new FluxSignalStatsRecorder(leaseHandler, requestType, requestOrReject.get());
          return response.doOnEach(recorder).doOnCancel(recorder);
        }
        return response;
      }
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Payload payload, Publisher<Payload> payloads) {
    try {
      ResponderLeaseHandler leaseHandler = this.leaseHandler;
      FrameType requestType = FrameType.REQUEST_CHANNEL;
      Signal<?> requestOrReject = leaseHandler.allowRequest(requestType, payload.metadata());
      Throwable reject = requestOrReject.getThrowable();
      if (reject != null) {
        payload.release();
        return Flux.error(reject);
      } else {
        Flux<Payload> response = super.requestChannel(payload, payloads);
        if (leaseHandler.requireStats()) {
          FluxSignalStatsRecorder recorder =
              new FluxSignalStatsRecorder(leaseHandler, requestType, requestOrReject.get());
          return response.doOnEach(recorder).doOnCancel(recorder);
        }
        return response;
      }
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  void terminate() {
    leaseDisposable.dispose();
    super.terminate();
  }

  @Override
  public void gracefulDispose(String msg) {
    logger.debug("Local graceful dispose RSocketResponder with message: {}, leases stopped", msg);
    leaseDisposable.dispose();
    super.gracefulDispose(msg);
  }
}
