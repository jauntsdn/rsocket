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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;

import com.jauntsdn.rsocket.RSocket;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class MicrometerRSocketInterceptorTest {

  private final RSocket delegate = mock(RSocket.class, RETURNS_SMART_NULLS);

  private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

  @DisplayName("creates MicrometerRSocket")
  @Test
  void apply() {
    assertThat(
            MicrometerRSocketInterceptors.create(meterRegistry)
                .requesterInterceptor()
                .apply(delegate))
        .isInstanceOf(MicrometerRSocket.class);
  }

  @DisplayName("apply throws NullPointerException with null delegate")
  @Test
  void applyNullDelegate() {
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                MicrometerRSocketInterceptors.create(meterRegistry)
                    .requesterInterceptor()
                    .apply(null))
        .withMessage("rSocket");
  }

  @DisplayName("constructor throws NullPointerException with null meterRegistry")
  @Test
  void constructorNullMeterRegistry() {
    assertThatNullPointerException()
        .isThrownBy(() -> MicrometerRSocketInterceptors.create(null))
        .withMessage("meterRegistry");
  }
}
