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

import static com.jauntsdn.rsocket.micrometer.MicrometerRSocket.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.jauntsdn.rsocket.Payload;
import com.jauntsdn.rsocket.RSocket;
import com.jauntsdn.rsocket.util.DefaultPayload;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

final class MicrometerRSocketTest {
  private static final String TAG_KEY = "test-key";
  private static final String TAG_VALUE = "test-value";

  private final RSocket delegate = mock(RSocket.class, RETURNS_SMART_NULLS);
  private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

  @DisplayName("fireAndForget gathers metrics")
  @Test
  void fireAndForget() {
    Payload payload = DefaultPayload.create("test-metadata", "test-data");
    when(delegate.fireAndForget(payload)).thenReturn(Mono.empty());

    new Requester(
            delegate, new ThreadLocalRSocketMeters(meterRegistry, Tags.of(TAG_KEY, TAG_VALUE)))
        .fireAndForget(payload)
        .as(StepVerifier::create)
        .verifyComplete();

    assertThat(
            findRequestStartedCounter(MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_FNF)
                .count())
        .isEqualTo(1);

    assertThat(
            findRequestCompletedCounter(
                    MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_FNF,
                    MicrometerRSocketInterceptor.TAG_SIGNAL_TYPE_COMPLETE)
                .count())
        .isEqualTo(1);
  }

  @DisplayName("metadataPush gathers metrics")
  @Test
  void metadataPush() {
    Payload payload = DefaultPayload.create("test-metadata", "test-data");
    when(delegate.metadataPush(payload)).thenReturn(Mono.empty());

    new Requester(
            delegate, new ThreadLocalRSocketMeters(meterRegistry, Tags.of(TAG_KEY, TAG_VALUE)))
        .metadataPush(payload)
        .as(StepVerifier::create)
        .verifyComplete();

    assertThat(
            findRequestStartedCounter(
                    MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_METADATA_PUSH)
                .count())
        .isEqualTo(1);

    assertThat(
            findRequestCompletedCounter(
                    MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_METADATA_PUSH,
                    MicrometerRSocketInterceptor.TAG_SIGNAL_TYPE_COMPLETE)
                .count())
        .isEqualTo(1);
  }

  @DisplayName("requestChannel gathers metrics")
  @Test
  void requestChannel() {
    Mono<Payload> payload = Mono.just(DefaultPayload.create("test-metadata", "test-data"));
    when(delegate.requestChannel(payload)).thenReturn(Flux.empty());

    new Requester(
            delegate, new ThreadLocalRSocketMeters(meterRegistry, Tags.of(TAG_KEY, TAG_VALUE)))
        .requestChannel(payload)
        .as(StepVerifier::create)
        .verifyComplete();

    assertThat(
            findRequestStartedCounter(MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_CHANNEL)
                .count())
        .isEqualTo(1);

    assertThat(
            findRequestCompletedCounter(
                    MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_CHANNEL,
                    MicrometerRSocketInterceptor.TAG_SIGNAL_TYPE_COMPLETE)
                .count())
        .isEqualTo(1);
  }

  @DisplayName("requestResponse gathers metrics")
  @Test
  void requestResponse() {
    Payload payload = DefaultPayload.create("test-metadata", "test-data");
    when(delegate.requestResponse(payload)).thenReturn(Mono.empty());

    new Requester(
            delegate, new ThreadLocalRSocketMeters(meterRegistry, Tags.of(TAG_KEY, TAG_VALUE)))
        .requestResponse(payload)
        .as(StepVerifier::create)
        .verifyComplete();

    assertThat(
            findRequestStartedCounter(MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_RESPONSE)
                .count())
        .isEqualTo(1);

    assertThat(
            findRequestCompletedCounter(
                    MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_RESPONSE,
                    MicrometerRSocketInterceptor.TAG_SIGNAL_TYPE_COMPLETE)
                .count())
        .isEqualTo(1);
  }

  @DisplayName("requestStream gathers metrics")
  @Test
  void requestStream() {
    Payload payload = DefaultPayload.create("test-metadata", "test-data");
    when(delegate.requestStream(payload)).thenReturn(Flux.empty());

    new Requester(
            delegate, new ThreadLocalRSocketMeters(meterRegistry, Tags.of(TAG_KEY, TAG_VALUE)))
        .requestStream(payload)
        .as(StepVerifier::create)
        .verifyComplete();

    assertThat(
            findRequestStartedCounter(MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_STREAM)
                .count())
        .isEqualTo(1);

    assertThat(
            findRequestCompletedCounter(
                    MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE_STREAM,
                    MicrometerRSocketInterceptor.TAG_SIGNAL_TYPE_COMPLETE)
                .count())
        .isEqualTo(1);
  }

  private Counter findRequestCompletedCounter(String interactionType, String signalType) {
    return meterRegistry
        .get(MicrometerRSocketInterceptor.COUNTER_RSOCKET_REQUEST_COMPLETED)
        .tag(MicrometerRSocketInterceptor.TAG_SIGNAL_TYPE, signalType)
        .tag(MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE, interactionType)
        .tag(TAG_KEY, TAG_VALUE)
        .counter();
  }

  private Counter findRequestStartedCounter(String interactionType) {
    return meterRegistry
        .get(MicrometerRSocketInterceptor.COUNTER_RSOCKET_REQUEST_STARTED)
        .tag(MicrometerRSocketInterceptor.TAG_INTERACTION_TYPE, interactionType)
        .tag(TAG_KEY, TAG_VALUE)
        .counter();
  }
}
