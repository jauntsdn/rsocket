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

package com.jauntsdn.rsocket.micrometer;

import static com.jauntsdn.rsocket.frame.FrameType.*;

import com.jauntsdn.rsocket.DuplexConnection;
import com.jauntsdn.rsocket.frame.FrameHeaderFlyweight;
import com.jauntsdn.rsocket.frame.FrameType;
import io.micrometer.core.instrument.*;
import io.netty.buffer.ByteBuf;
import java.util.Objects;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * An implementation of {@link DuplexConnection} that intercepts frames and gathers Micrometer
 * metrics about them.
 *
 * <p>The metric is called {@code rsocket.frame} and is tagged with {@code frame.type} ({@link
 * FrameType}), and any additional configured tags. {@code rsocket.duplex.connection.close} and
 * {@code rsocket.duplex.connection.dispose} metrics. Any additional configured tags are also
 * collected.
 *
 * @see <a href="https://micrometer.io">Micrometer</a>
 */
final class MicrometerDuplexConnection implements DuplexConnection {

  private final Counter close;

  private final DuplexConnection delegate;

  private final Counter dispose;

  private final FrameCounters frameCounters;

  /**
   * Creates a new {@link DuplexConnection}.
   *
   * @param delegate the {@link DuplexConnection} to delegate to
   * @param meterRegistry the {@link MeterRegistry} to use
   * @param tags additional tags to attach to {@link Meter}s
   * @throws NullPointerException if {@code connectionType}, {@code delegate}, or {@code
   *     meterRegistry} is {@code null}
   */
  MicrometerDuplexConnection(DuplexConnection delegate, MeterRegistry meterRegistry, Tag... tags) {

    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");

    this.close = meterRegistry.counter("rsocket.duplex.connection.close", Tags.of(tags));
    this.dispose = meterRegistry.counter("rsocket.duplex.connection.dispose", Tags.of(tags));
    this.frameCounters = new FrameCounters(meterRegistry, tags);
  }

  @Override
  public void dispose() {
    delegate.dispose();
    dispose.increment();
  }

  @Override
  public Mono<Void> onClose() {
    return delegate.onClose().doAfterTerminate(close::increment);
  }

  @Override
  public Flux<ByteBuf> receive() {
    return delegate.receive().doOnNext(frameCounters);
  }

  @Override
  public Scheduler scheduler() {
    return delegate.scheduler();
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    Objects.requireNonNull(frames, "frames must not be null");

    return delegate.send(Flux.from(frames).doOnNext(frameCounters));
  }

  private static final class FrameCounters implements Consumer<ByteBuf> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Counter cancel;

    private final Counter complete;

    private final Counter error;

    private final Counter extension;

    private final Counter keepalive;

    private final Counter lease;

    private final Counter metadataPush;

    private final Counter next;

    private final Counter nextComplete;

    private final Counter payload;

    private final Counter requestChannel;

    private final Counter requestFireAndForget;

    private final Counter requestN;

    private final Counter requestResponse;

    private final Counter requestStream;

    private final Counter resume;

    private final Counter resumeOk;

    private final Counter setup;

    private final Counter unknown;

    private FrameCounters(MeterRegistry meterRegistry, Tag... tags) {
      this.cancel = counter(meterRegistry, CANCEL, tags);
      this.complete = counter(meterRegistry, COMPLETE, tags);
      this.error = counter(meterRegistry, ERROR, tags);
      this.extension = counter(meterRegistry, EXT, tags);
      this.keepalive = counter(meterRegistry, KEEPALIVE, tags);
      this.lease = counter(meterRegistry, LEASE, tags);
      this.metadataPush = counter(meterRegistry, METADATA_PUSH, tags);
      this.next = counter(meterRegistry, NEXT, tags);
      this.nextComplete = counter(meterRegistry, NEXT_COMPLETE, tags);
      this.payload = counter(meterRegistry, PAYLOAD, tags);
      this.requestChannel = counter(meterRegistry, REQUEST_CHANNEL, tags);
      this.requestFireAndForget = counter(meterRegistry, REQUEST_FNF, tags);
      this.requestN = counter(meterRegistry, REQUEST_N, tags);
      this.requestResponse = counter(meterRegistry, REQUEST_RESPONSE, tags);
      this.requestStream = counter(meterRegistry, REQUEST_STREAM, tags);
      this.resume = counter(meterRegistry, RESUME, tags);
      this.resumeOk = counter(meterRegistry, RESUME_OK, tags);
      this.setup = counter(meterRegistry, SETUP, tags);
      this.unknown = counter(meterRegistry, "UNKNOWN", tags);
    }

    private static Counter counter(MeterRegistry meterRegistry, FrameType frameType, Tag... tags) {

      return counter(meterRegistry, frameType.name(), tags);
    }

    private static Counter counter(MeterRegistry meterRegistry, String frameType, Tag... tags) {

      return meterRegistry.counter("rsocket.frame", Tags.of(tags).and("frame.type", frameType));
    }

    @Override
    public void accept(ByteBuf frame) {
      FrameType frameType = FrameHeaderFlyweight.frameType(frame);

      switch (frameType) {
        case SETUP:
          this.setup.increment();
          break;
        case LEASE:
          this.lease.increment();
          break;
        case KEEPALIVE:
          this.keepalive.increment();
          break;
        case REQUEST_RESPONSE:
          this.requestResponse.increment();
          break;
        case REQUEST_FNF:
          this.requestFireAndForget.increment();
          break;
        case REQUEST_STREAM:
          this.requestStream.increment();
          break;
        case REQUEST_CHANNEL:
          this.requestChannel.increment();
          break;
        case REQUEST_N:
          this.requestN.increment();
          break;
        case CANCEL:
          this.cancel.increment();
          break;
        case PAYLOAD:
          this.payload.increment();
          break;
        case ERROR:
          this.error.increment();
          break;
        case METADATA_PUSH:
          this.metadataPush.increment();
          break;
        case RESUME:
          this.resume.increment();
          break;
        case RESUME_OK:
          this.resumeOk.increment();
          break;
        case NEXT:
          this.next.increment();
          break;
        case COMPLETE:
          this.complete.increment();
          break;
        case NEXT_COMPLETE:
          this.nextComplete.increment();
          break;
        case EXT:
          this.extension.increment();
          break;
        default:
          this.logger.debug("Skipping count of unknown frame type: {}", frameType);
          this.unknown.increment();
      }
    }
  }
}
