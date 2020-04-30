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

import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.*;

import com.jauntsdn.rsocket.DuplexConnection;
import com.jauntsdn.rsocket.test.TestFrames;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

final class MicrometerDuplexConnectionTest {
  private final DuplexConnection delegate = mock(DuplexConnection.class, RETURNS_SMART_NULLS);
  private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

  @Test
  void openConnection() {
    MicrometerDuplexConnectionInterceptor interceptor =
        MicrometerDuplexConnectionInterceptors.create(
                meterRegistry, Tag.of("test-key", "test-value"))
            .interceptor();
    DuplexConnection micrometerConnection = interceptor.apply(delegate);

    Assertions.assertThat(
            meterRegistry
                .get(MicrometerDuplexConnectionInterceptor.COUNTER_CONNECTIONS_OPENED)
                .tag("test-key", "test-value")
                .counter()
                .count())
        .isEqualTo(1);
  }

  @Test
  void closeConnection() {
    when(delegate.onClose()).thenReturn(Mono.empty());

    MicrometerDuplexConnectionInterceptor interceptor =
        MicrometerDuplexConnectionInterceptors.create(
                meterRegistry, Tag.of("test-key", "test-value"))
            .interceptor();
    DuplexConnection micrometerConnection = interceptor.apply(delegate);

    micrometerConnection.onClose().subscribe(Operators.drainSubscriber());

    Assertions.assertThat(
            meterRegistry
                .get(MicrometerDuplexConnectionInterceptor.COUNTER_CONNECTIONS_CLOSED)
                .tag("test-key", "test-value")
                .counter()
                .count())
        .isEqualTo(1);
  }

  @Test
  void inboundFrames() {
    List<ByteBuf> frames =
        Arrays.asList(
            TestFrames.createTestCancelFrame(),
            TestFrames.createTestErrorFrame(),
            TestFrames.createTestKeepaliveFrame(),
            TestFrames.createTestLeaseFrame(),
            TestFrames.createTestMetadataPushFrame(),
            TestFrames.createTestPayloadFrame(),
            TestFrames.createTestRequestChannelFrame(),
            TestFrames.createTestRequestFireAndForgetFrame(),
            TestFrames.createTestRequestNFrame(),
            TestFrames.createTestRequestResponseFrame(),
            TestFrames.createTestRequestStreamFrame(),
            TestFrames.createTestSetupFrame());

    int expectedTotalSize = frames.stream().mapToInt(ByteBuf::readableBytes).sum();
    int expectedCount = frames.size();

    Flux<ByteBuf> framesFlux = Flux.fromIterable(frames);

    when(delegate.receive()).thenReturn(framesFlux);

    MicrometerDuplexConnectionInterceptor interceptor =
        MicrometerDuplexConnectionInterceptors.create(meterRegistry).interceptor();
    DuplexConnection micrometerConnection = interceptor.apply(delegate);
    micrometerConnection
        .receive()
        .doOnNext(ReferenceCounted::release)
        .subscribe(Operators.drainSubscriber());

    Assertions.assertThat(
            meterRegistry
                .get(MicrometerDuplexConnectionInterceptor.COUNTER_FRAMES_COUNT)
                .tag(
                    MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION,
                    MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION_INBOUND)
                .counter()
                .count())
        .isEqualTo(expectedCount);

    org.junit.jupiter.api.Assertions.assertThrows(
        MeterNotFoundException.class,
        () -> {
          meterRegistry
              .get(MicrometerDuplexConnectionInterceptor.COUNTER_FRAMES_COUNT)
              .tag(
                  MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION,
                  MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION_OUTBOUND)
              .counters();
        });

    Assertions.assertThat(
            meterRegistry
                .get(MicrometerDuplexConnectionInterceptor.COUNTER_FRAMES_SIZE)
                .tag(
                    MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION,
                    MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION_INBOUND)
                .counter()
                .count())
        .isEqualTo(expectedTotalSize);

    org.junit.jupiter.api.Assertions.assertThrows(
        MeterNotFoundException.class,
        () -> {
          meterRegistry
              .get(MicrometerDuplexConnectionInterceptor.COUNTER_FRAMES_SIZE)
              .tag(
                  MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION,
                  MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION_OUTBOUND)
              .counters();
        });
  }

  @SuppressWarnings("unchecked")
  @Test
  void outboundFrames() {
    ArgumentCaptor<Publisher<ByteBuf>> captor = ArgumentCaptor.forClass(Publisher.class);
    when(delegate.send(captor.capture())).thenReturn(Mono.empty());

    List<ByteBuf> frames =
        Arrays.asList(
            TestFrames.createTestCancelFrame(),
            TestFrames.createTestErrorFrame(),
            TestFrames.createTestKeepaliveFrame(),
            TestFrames.createTestLeaseFrame(),
            TestFrames.createTestMetadataPushFrame(),
            TestFrames.createTestPayloadFrame(),
            TestFrames.createTestRequestChannelFrame(),
            TestFrames.createTestRequestFireAndForgetFrame(),
            TestFrames.createTestRequestNFrame(),
            TestFrames.createTestRequestResponseFrame(),
            TestFrames.createTestRequestStreamFrame(),
            TestFrames.createTestSetupFrame());

    int expectedTotalSize = frames.stream().mapToInt(ByteBuf::readableBytes).sum();
    int expectedCount = frames.size();

    MicrometerDuplexConnectionInterceptor interceptor =
        MicrometerDuplexConnectionInterceptors.create(meterRegistry).interceptor();
    DuplexConnection micrometerConnection = interceptor.apply(delegate);
    micrometerConnection.send(Flux.fromIterable(frames)).as(StepVerifier::create).verifyComplete();

    Publisher<ByteBuf> sent = Flux.from(captor.getValue()).doOnNext(ReferenceCounted::release);
    StepVerifier.create(sent).expectNextCount(12).verifyComplete();

    Assertions.assertThat(
            meterRegistry
                .get(MicrometerDuplexConnectionInterceptor.COUNTER_FRAMES_COUNT)
                .tag(
                    MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION,
                    MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION_OUTBOUND)
                .counter()
                .count())
        .isEqualTo(expectedCount);

    org.junit.jupiter.api.Assertions.assertThrows(
        MeterNotFoundException.class,
        () -> {
          meterRegistry
              .get(MicrometerDuplexConnectionInterceptor.COUNTER_FRAMES_COUNT)
              .tag(
                  MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION,
                  MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION_INBOUND)
              .counters();
        });

    Assertions.assertThat(
            meterRegistry
                .get(MicrometerDuplexConnectionInterceptor.COUNTER_FRAMES_SIZE)
                .tag(
                    MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION,
                    MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION_OUTBOUND)
                .counter()
                .count())
        .isEqualTo(expectedTotalSize);

    org.junit.jupiter.api.Assertions.assertThrows(
        MeterNotFoundException.class,
        () -> {
          meterRegistry
              .get(MicrometerDuplexConnectionInterceptor.COUNTER_FRAMES_SIZE)
              .tag(
                  MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION,
                  MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION_INBOUND)
              .counters();
        });
  }

  @Test
  void outboundNullFrames() {
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                MicrometerDuplexConnectionInterceptors.create(meterRegistry)
                    .interceptor()
                    .apply(delegate)
                    .send(null))
        .withMessage("frames");
  }

  @Test
  void inboundMetersCount() {
    int elementsCount = 2;
    Flux<ByteBuf> source = multiThreadedFramesSource(elementsCount);

    when(delegate.receive()).thenReturn(source);

    MicrometerDuplexConnectionInterceptor interceptor =
        MicrometerDuplexConnectionInterceptors.create(meterRegistry).interceptor();
    DuplexConnection micrometerConnection = interceptor.apply(delegate);
    micrometerConnection
        .receive()
        .doOnNext(ReferenceCounted::release)
        .as(StepVerifier::create)
        .expectNextCount(elementsCount)
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    Assertions.assertThat(
            meterRegistry.get(MicrometerDuplexConnectionInterceptor.COUNTER_FRAMES_SIZE).counters())
        .hasSize(1);
    Assertions.assertThat(
            meterRegistry
                .get(MicrometerDuplexConnectionInterceptor.COUNTER_FRAMES_COUNT)
                .counters())
        .hasSize(1);
  }

  @Test
  @SuppressWarnings("unchecked")
  void outboundMetersCount() {
    ArgumentCaptor<Publisher<ByteBuf>> captor = ArgumentCaptor.forClass(Publisher.class);
    when(delegate.send(captor.capture())).thenReturn(Mono.empty());

    int elementsCount = 2;
    Flux<ByteBuf> source = multiThreadedFramesSource(elementsCount);

    MicrometerDuplexConnectionInterceptor interceptor =
        MicrometerDuplexConnectionInterceptors.create(meterRegistry).interceptor();
    DuplexConnection micrometerConnection = interceptor.apply(delegate);

    micrometerConnection
        .send(source)
        .as(StepVerifier::create)
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    Publisher<ByteBuf> sent = Flux.from(captor.getValue()).doOnNext(ReferenceCounted::release);

    StepVerifier.create(sent)
        .expectNextCount(elementsCount)
        .expectComplete()
        .verify(Duration.ofSeconds(5));
    Assertions.assertThat(
            meterRegistry.get(MicrometerDuplexConnectionInterceptor.COUNTER_FRAMES_SIZE).counters())
        .hasSize(1);
    Assertions.assertThat(
            meterRegistry
                .get(MicrometerDuplexConnectionInterceptor.COUNTER_FRAMES_COUNT)
                .counters())
        .hasSize(1);
  }

  private static Flux<ByteBuf> multiThreadedFramesSource(int elementsCount) {
    if (elementsCount == 0) {
      return Flux.empty();
    }
    Flux<ByteBuf> source =
        Flux.create(
            sink -> {
              sink.next(TestFrames.createTestRequestResponseFrame());
              sink.complete();
            });

    Flux<ByteBuf> result = source;
    for (int i = 0; i < elementsCount - 1; i++) {
      result = result.mergeWith(source.subscribeOn(Schedulers.newSingle("scheduler-" + i)));
    }
    return result;
  }
}
