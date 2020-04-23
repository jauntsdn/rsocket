/*
 * Copyright 2020 - present Maksym Ostroverkhov.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jauntsdn.rsocket.micrometer;

import com.jauntsdn.rsocket.Payload;
import com.jauntsdn.rsocket.RSocket;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.noop.NoopCounter;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.Optional;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;

abstract class MicrometerRSocket implements RSocket {
  private static final Logger logger = LoggerFactory.getLogger(MicrometerRSocket.class);

  private final RSocket delegate;
  private final ThreadLocalRSocketMeters meters;

  MicrometerRSocket(RSocket rSocket, ThreadLocalRSocketMeters meters) {
    this.delegate = rSocket;
    this.meters = meters;
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return delegate.metadataPush(payload).doFinally(meters::meterMetadataPush);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return delegate.fireAndForget(payload).doFinally(meters::meterFireAndForget);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return delegate.requestResponse(payload).doFinally(meters::meterRequestResponse);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return delegate.requestStream(payload).doFinally(meters::meterRequestStream);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return delegate.requestChannel(payloads).doFinally(meters::meterRequestChannel);
  }

  @Override
  public void dispose() {
    delegate.dispose();
  }

  @Override
  public Mono<Void> onClose() {
    return delegate.onClose();
  }

  @Override
  public Optional<Scheduler> scheduler() {
    return delegate.scheduler();
  }

  final void meterMetadataPush() {
    meters.meterMetadataPush(SignalType.ON_SUBSCRIBE);
  }

  final void meterFireAndForget() {
    meters.meterFireAndForget(SignalType.ON_SUBSCRIBE);
  }

  final void meterRequestResponse() {
    meters.meterRequestResponse(SignalType.ON_SUBSCRIBE);
  }

  final void meterRequestStream() {
    meters.meterRequestStream(SignalType.ON_SUBSCRIBE);
  }

  final void meterRequestChannel() {
    meters.meterRequestChannel(SignalType.ON_SUBSCRIBE);
  }

  static final class Handler extends MicrometerRSocket {

    Handler(RSocket delegate, ThreadLocalRSocketMeters meters) {
      super(delegate, meters);
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
      meterMetadataPush();
      return super.metadataPush(payload);
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      meterFireAndForget();
      return super.fireAndForget(payload);
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      meterRequestResponse();
      return super.requestResponse(payload);
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      meterRequestStream();
      return super.requestStream(payload);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      meterRequestChannel();
      return super.requestChannel(payloads);
    }
  }

  static final class Requester extends MicrometerRSocket {

    Requester(RSocket delegate, ThreadLocalRSocketMeters meters) {
      super(delegate, meters);
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
      return super.metadataPush(payload).doOnSubscribe(s -> meterMetadataPush());
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      return super.fireAndForget(payload).doOnSubscribe(s -> meterFireAndForget());
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return super.requestResponse(payload).doOnSubscribe(s -> meterRequestResponse());
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      return super.requestStream(payload).doOnSubscribe(s -> meterRequestStream());
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return super.requestChannel(payloads).doOnSubscribe(s -> meterRequestChannel());
    }
  }

  static final class ThreadLocalRSocketMeters {
    // thread local per instance, instance per tags set
    private final FastThreadLocal<RSocketCounters> threadLocalCounters =
        new FastThreadLocal<RSocketCounters>() {
          @Override
          protected RSocketCounters initialValue() {
            return new RSocketCounters(registry, tags);
          }
        };

    private final MeterRegistry registry;
    private final Tags tags;

    ThreadLocalRSocketMeters(MeterRegistry registry, Tags tags) {
      this.registry = registry;
      this.tags = tags;
    }

    public void meterMetadataPush(SignalType signalType) {
      counters().metadataPush(signalType).increment();
    }

    public void meterFireAndForget(SignalType signalType) {
      counters().fireAndForget(signalType).increment();
    }

    public void meterRequestResponse(SignalType signalType) {
      counters().requestResponse(signalType).increment();
    }

    public void meterRequestStream(SignalType signalType) {
      counters().requestStream(signalType).increment();
    }

    public void meterRequestChannel(SignalType signalType) {
      counters().requestChannel(signalType).increment();
    }

    private RSocketCounters counters() {
      return threadLocalCounters.get();
    }

    private static class RSocketCounters {
      private final SignalCounters metadataPush;
      private final SignalCounters fnf;
      private final SignalCounters requestResponse;
      private final SignalCounters requestStream;
      private final SignalCounters requestChannel;

      public RSocketCounters(MeterRegistry meterRegistry, Tags tags) {
        this.metadataPush =
            new SignalCounters(
                meterRegistry,
                tags,
                MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_METADATA_PUSH);
        this.fnf =
            new SignalCounters(
                meterRegistry, tags, MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_FNF);
        this.requestResponse =
            new SignalCounters(
                meterRegistry, tags, MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_RESPONSE);
        this.requestStream =
            new SignalCounters(
                meterRegistry, tags, MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_STREAM);
        this.requestChannel =
            new SignalCounters(
                meterRegistry, tags, MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_CHANNEL);
      }

      public Counter metadataPush(SignalType signalType) {
        return metadataPush.signal(signalType);
      }

      public Counter fireAndForget(SignalType signalType) {
        return fnf.signal(signalType);
      }

      public Counter requestResponse(SignalType signalType) {
        return requestResponse.signal(signalType);
      }

      public Counter requestStream(SignalType signalType) {
        return requestStream.signal(signalType);
      }

      public Counter requestChannel(SignalType signalType) {
        return requestChannel.signal(signalType);
      }

      private static class SignalCounters {
        private static final Counter NOOP = new NoopCounter(null);

        private final Counter onSubscribe;
        private final Counter onComplete;
        private final Counter onError;
        private final Counter onCancel;

        public SignalCounters(MeterRegistry registry, Tags tags, String interactionType) {
          this.onSubscribe = createStartedCounter(registry, tags, interactionType);
          this.onComplete =
              createCompletedCounter(
                  registry,
                  tags,
                  interactionType,
                  MicrometerRSocketInterceptor.TAG_SIGNAL_TYPE_COMPLETE);
          this.onError =
              createCompletedCounter(
                  registry,
                  tags,
                  interactionType,
                  MicrometerRSocketInterceptor.TAG_SIGNAL_TYPE_ERROR);
          this.onCancel =
              createCompletedCounter(
                  registry,
                  tags,
                  interactionType,
                  MicrometerRSocketInterceptor.TAG_SIGNAL_TYPE_CANCEL);
        }

        public Counter signal(SignalType signalType) {
          switch (signalType) {
            case ON_SUBSCRIBE:
              return onSubscribe;
            case ON_COMPLETE:
              return onComplete;
            case ON_ERROR:
              return onError;
            case CANCEL:
              return onCancel;
            default:
              logger.info("Unexpected signal type: " + signalType);
              return NOOP;
          }
        }

        private static Counter createStartedCounter(
            MeterRegistry meterRegistry, Tags tags, String interactionType) {
          return meterRegistry.counter(
              MicrometerRSocketInterceptor.COUNTER_RSOCKET_REQUEST_STARTED,
              tags.and(MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE, interactionType));
        }

        private static Counter createCompletedCounter(
            MeterRegistry meterRegistry, Tags tags, String interactionType, String signalType) {
          return meterRegistry.counter(
              MicrometerRSocketInterceptor.COUNTER_RSOCKET_REQUEST_COMPLETED,
              tags.and(MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE, interactionType)
                  .and(MicrometerRSocketInterceptor.TAG_SIGNAL_TYPE, signalType));
        }
      }
    }
  }
}
