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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.Objects;

/**
 * Creates RSocket interceptors instrumented with Micrometer {@link
 * com.jauntsdn.rsocket.micrometer.MicrometerRSocketInterceptor}
 */
public final class MicrometerRSocketInterceptors {
  private final MeterRegistry meterRegistry;
  private final Tags tags;
  private MicrometerRSocketInterceptor requesterInterceptor;
  private MicrometerRSocketInterceptor handlerInterceptor;

  private MicrometerRSocketInterceptors(MeterRegistry meterRegistry, Tag[] tags) {
    this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
    this.tags = Tags.of(tags);
  }

  /**
   * @param meterRegistry meters registry
   * @param tags Set of tags added to every meter of RSocket interceptor
   * @return MicrometerRSocketInterceptors instance for creating {@link
   *     MicrometerRSocketInterceptor}
   */
  public static MicrometerRSocketInterceptors create(MeterRegistry meterRegistry, Tag... tags) {
    return new MicrometerRSocketInterceptors(meterRegistry, tags);
  }

  /**
   * @return {@link MicrometerRSocketInterceptor} instance for instrumenting RSocket handlers with
   *     Micrometer
   */
  public MicrometerRSocketInterceptor handlerInterceptor() {
    MicrometerRSocketInterceptor i = handlerInterceptor;
    if (i == null) {
      Tags handlerTags =
          tags.and(
              MicrometerRSocketInterceptor.TAG_RSOCKET_ROLE,
              MicrometerRSocketInterceptor.TAG_RSOCKET_ROLE_RESPONDER);

      MicrometerRSocket.ThreadLocalRSocketMeters handlerMeters =
          new MicrometerRSocket.ThreadLocalRSocketMeters(meterRegistry, handlerTags);
      i =
          handlerInterceptor =
              rSocket ->
                  new MicrometerRSocket.Handler(
                      Objects.requireNonNull(rSocket, "rSocket"), handlerMeters);
    }
    return i;
  }

  /**
   * @return {@link MicrometerRSocketInterceptor} instance for instrumenting RSocket requesters with
   *     Micrometer
   */
  public MicrometerRSocketInterceptor requesterInterceptor() {
    MicrometerRSocketInterceptor i = requesterInterceptor;
    if (i == null) {
      Tags requesterTags =
          tags.and(
              MicrometerRSocketInterceptor.TAG_RSOCKET_ROLE,
              MicrometerRSocketInterceptor.TAG_RSOCKET_ROLE_REQUESTER);
      MicrometerRSocket.ThreadLocalRSocketMeters requesterMeters =
          new MicrometerRSocket.ThreadLocalRSocketMeters(meterRegistry, requesterTags);
      i =
          requesterInterceptor =
              rSocket ->
                  new MicrometerRSocket.Requester(
                      Objects.requireNonNull(rSocket, "rSocket"), requesterMeters);
    }
    return i;
  }
}
