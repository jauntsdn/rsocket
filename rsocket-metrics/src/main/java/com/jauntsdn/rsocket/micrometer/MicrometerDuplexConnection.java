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

import com.jauntsdn.rsocket.DuplexConnection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

final class MicrometerDuplexConnection extends AtomicBoolean implements DuplexConnection {
  private final DuplexConnection delegate;
  private final ThreadLocalFrameMeters inboundFrames;
  private final ThreadLocalFrameMeters outboundFrames;
  private final ThreadLocalConnectionMeters connectionMeters;

  MicrometerDuplexConnection(
      DuplexConnection connection,
      ThreadLocalConnectionMeters connectionMeters,
      ThreadLocalFrameMeters inboundFrames,
      ThreadLocalFrameMeters outboundFrames) {
    this.delegate = connection;
    this.connectionMeters = connectionMeters;
    this.inboundFrames = inboundFrames;
    this.outboundFrames = outboundFrames;

    connectionMeters.meterOpenedConnections();
  }

  @Override
  public void dispose() {
    delegate.dispose();
  }

  @Override
  public Mono<Void> onClose() {
    return delegate
        .onClose()
        .doAfterTerminate(
            () -> {
              if (compareAndSet(false, true)) {
                connectionMeters.meterClosedConnections();
              }
            });
  }

  @Override
  public Flux<ByteBuf> receive() {
    return delegate.receive().doOnNext(inboundFrames);
  }

  @Override
  public Scheduler scheduler() {
    return delegate.scheduler();
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    Objects.requireNonNull(frames, "frames");

    return delegate.send(Flux.from(frames).doOnNext(outboundFrames));
  }

  static final class ThreadLocalConnectionMeters {
    // thread local per instance, instance per tags set
    private final FastThreadLocal<ConnectionCounters> threadLocalCounters =
        new FastThreadLocal<ConnectionCounters>() {
          @Override
          protected ConnectionCounters initialValue() {
            return new ConnectionCounters(registry, tags);
          }
        };

    private final MeterRegistry registry;
    private final Tags tags;

    ThreadLocalConnectionMeters(MeterRegistry registry, Tags tags) {
      this.registry = registry;
      this.tags = tags;
    }

    public void meterOpenedConnections() {
      threadLocalCounters.get().openedConnections().increment();
    }

    public void meterClosedConnections() {
      threadLocalCounters.get().closedConnections().increment();
    }
  }

  static final class ThreadLocalFrameMeters implements Consumer<ByteBuf> {
    // thread local per instance, instance per tags set
    private final FastThreadLocal<FrameCounter> threadLocalCounters =
        new FastThreadLocal<FrameCounter>() {
          @Override
          protected FrameCounter initialValue() {
            return new FrameCounter(registry, tags);
          }
        };

    private final MeterRegistry registry;
    private final Tags tags;

    ThreadLocalFrameMeters(MeterRegistry registry, Tags tags) {
      this.registry = registry;
      this.tags = tags;
    }

    @Override
    public void accept(ByteBuf frame) {
      FrameCounter counter = threadLocalCounters.get();
      counter.increment(frame);
    }
  }

  static final class ConnectionCounters {
    private final Counter openedConnections;
    private final Counter closedConnections;

    public ConnectionCounters(MeterRegistry registry, Tags tags) {
      this.openedConnections =
          registry.counter(
              MicrometerDuplexConnectionInterceptor.COUNTER_CONNECTIONS_OPENED, Tags.of(tags));
      this.closedConnections =
          registry.counter(
              MicrometerDuplexConnectionInterceptor.COUNTER_CONNECTIONS_CLOSED, Tags.of(tags));
    }

    public Counter openedConnections() {
      return openedConnections;
    }

    public Counter closedConnections() {
      return closedConnections;
    }
  }

  static final class FrameCounter {
    private final Counter framesCount;
    private final Counter framesSize;

    public FrameCounter(MeterRegistry registry, Tags tags) {
      this.framesCount =
          registry.counter(
              MicrometerDuplexConnectionInterceptor.COUNTER_FRAMES_COUNT, Tags.of(tags));
      this.framesSize =
          registry.counter(
              MicrometerDuplexConnectionInterceptor.COUNTER_FRAMES_SIZE, Tags.of(tags));
    }

    public void increment(ByteBuf frame) {
      framesCount.increment();
      framesSize.increment(frame.readableBytes());
    }
  }
}
