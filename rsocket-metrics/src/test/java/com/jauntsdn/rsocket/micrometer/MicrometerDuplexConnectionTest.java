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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.*;

import com.jauntsdn.rsocket.DuplexConnection;
import com.jauntsdn.rsocket.frame.FrameType;
import com.jauntsdn.rsocket.test.TestFrames;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.test.StepVerifier;

final class MicrometerDuplexConnectionTest {

  private final DuplexConnection delegate = mock(DuplexConnection.class, RETURNS_SMART_NULLS);

  private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

  @DisplayName("constructor throws NullPointerException with null delegate")
  @Test
  void constructorNullDelegate() {
    assertThatNullPointerException()
        .isThrownBy(() -> new MicrometerDuplexConnection(null, meterRegistry))
        .withMessage("delegate must not be null");
  }

  @DisplayName("constructor throws NullPointerException with null meterRegistry")
  @Test
  void constructorNullMeterRegistry() {

    assertThatNullPointerException()
        .isThrownBy(() -> new MicrometerDuplexConnection(delegate, null))
        .withMessage("meterRegistry must not be null");
  }

  @DisplayName("dispose gathers metrics")
  @Test
  void dispose() {
    new MicrometerDuplexConnection(delegate, meterRegistry, Tag.of("test-key", "test-value"))
        .dispose();

    assertThat(
            meterRegistry
                .get("rsocket.duplex.connection.dispose")
                .tag("test-key", "test-value")
                .counter()
                .count())
        .isEqualTo(1);
  }

  @DisplayName("onClose gathers metrics")
  @Test
  void onClose() {
    when(delegate.onClose()).thenReturn(Mono.empty());

    new MicrometerDuplexConnection(delegate, meterRegistry, Tag.of("test-key", "test-value"))
        .onClose()
        .subscribe(Operators.drainSubscriber());

    assertThat(
            meterRegistry
                .get("rsocket.duplex.connection.close")
                .tag("test-key", "test-value")
                .counter()
                .count())
        .isEqualTo(1);
  }

  @DisplayName("receive gathers metrics")
  @Test
  void receive() {
    Flux<ByteBuf> frames =
        Flux.just(
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

    when(delegate.receive()).thenReturn(frames);

    new MicrometerDuplexConnection(delegate, meterRegistry, Tag.of("test-key", "test-value"))
        .receive()
        .as(StepVerifier::create)
        .expectNextCount(12)
        .verifyComplete();

    assertThat(findCounter(CANCEL).count()).isEqualTo(1);
    assertThat(findCounter(COMPLETE).count()).isEqualTo(1);
    assertThat(findCounter(ERROR).count()).isEqualTo(1);
    assertThat(findCounter(KEEPALIVE).count()).isEqualTo(1);
    assertThat(findCounter(LEASE).count()).isEqualTo(1);
    assertThat(findCounter(METADATA_PUSH).count()).isEqualTo(1);
    assertThat(findCounter(REQUEST_CHANNEL).count()).isEqualTo(1);
    assertThat(findCounter(REQUEST_FNF).count()).isEqualTo(1);
    assertThat(findCounter(REQUEST_N).count()).isEqualTo(1);
    assertThat(findCounter(REQUEST_RESPONSE).count()).isEqualTo(1);
    assertThat(findCounter(REQUEST_STREAM).count()).isEqualTo(1);
    assertThat(findCounter(SETUP).count()).isEqualTo(1);
  }

  @DisplayName("send gathers metrics")
  @SuppressWarnings("unchecked")
  @Test
  void send() {
    ArgumentCaptor<Publisher<ByteBuf>> captor = ArgumentCaptor.forClass(Publisher.class);
    when(delegate.send(captor.capture())).thenReturn(Mono.empty());

    Flux<ByteBuf> frames =
        Flux.just(
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

    new MicrometerDuplexConnection(delegate, meterRegistry, Tag.of("test-key", "test-value"))
        .send(frames)
        .as(StepVerifier::create)
        .verifyComplete();

    StepVerifier.create(captor.getValue()).expectNextCount(12).verifyComplete();

    assertThat(findCounter(CANCEL).count()).isEqualTo(1);
    assertThat(findCounter(COMPLETE).count()).isEqualTo(1);
    assertThat(findCounter(ERROR).count()).isEqualTo(1);
    assertThat(findCounter(KEEPALIVE).count()).isEqualTo(1);
    assertThat(findCounter(LEASE).count()).isEqualTo(1);
    assertThat(findCounter(METADATA_PUSH).count()).isEqualTo(1);
    assertThat(findCounter(REQUEST_CHANNEL).count()).isEqualTo(1);
    assertThat(findCounter(REQUEST_FNF).count()).isEqualTo(1);
    assertThat(findCounter(REQUEST_N).count()).isEqualTo(1);
    assertThat(findCounter(REQUEST_RESPONSE).count()).isEqualTo(1);
    assertThat(findCounter(REQUEST_STREAM).count()).isEqualTo(1);
    assertThat(findCounter(SETUP).count()).isEqualTo(1);
  }

  @DisplayName("send throws NullPointerException with null frames")
  @Test
  void sendNullFrames() {
    assertThatNullPointerException()
        .isThrownBy(() -> new MicrometerDuplexConnection(delegate, meterRegistry).send(null))
        .withMessage("frames must not be null");
  }

  private Counter findCounter(FrameType frameType) {
    return meterRegistry
        .get("rsocket.frame")
        .tag("frame.type", frameType.name())
        .tag("test-key", "test-value")
        .counter();
  }
}
