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

import static com.jauntsdn.rsocket.micrometer.MicrometerDuplexConnection.*;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.Objects;

/**
 * Creates Micrometer instrumented DuplexConnection interceptors {@link
 * com.jauntsdn.rsocket.micrometer.MicrometerDuplexConnectionInterceptor}
 */
public final class MicrometerDuplexConnectionInterceptors {
  private final MeterRegistry registry;
  private final Tags tags;
  private MicrometerDuplexConnectionInterceptor connectionInterceptor;

  private MicrometerDuplexConnectionInterceptors(MeterRegistry registry, Tag... tags) {
    this.registry = Objects.requireNonNull(registry, "meterRegistry");
    this.tags = Tags.of(tags);
  }

  /**
   * @param meterRegistry meters registry
   * @param tags Set of tags added to every meter of {@link MicrometerDuplexConnectionInterceptor}
   * @return MicrometerDuplexConnectionInterceptors instance for creating {@link
   *     MicrometerDuplexConnectionInterceptor}
   */
  public static MicrometerDuplexConnectionInterceptors create(
      MeterRegistry meterRegistry, Tag... tags) {
    return new MicrometerDuplexConnectionInterceptors(meterRegistry, tags);
  }

  /**
   * @return {@link MicrometerDuplexConnectionInterceptor} instance for instrumenting RSocket
   *     connections with Micrometer
   */
  public MicrometerDuplexConnectionInterceptor interceptor() {
    MicrometerDuplexConnectionInterceptor i = connectionInterceptor;
    if (i == null) {
      Tags connectionTags = tags;

      Tags inboundFramesTags =
          connectionTags.and(
              MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION,
              MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION_INBOUND);

      Tags outboundFramesTags =
          connectionTags.and(
              MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION,
              MicrometerDuplexConnectionInterceptor.TAG_FRAME_DIRECTION_OUTBOUND);

      ThreadLocalConnectionMeters connectionMeters =
          new ThreadLocalConnectionMeters(registry, connectionTags);

      ThreadLocalFrameMeters inboundFrameMeters =
          new ThreadLocalFrameMeters(registry, inboundFramesTags);

      ThreadLocalFrameMeters outboundFrameMeters =
          new ThreadLocalFrameMeters(registry, outboundFramesTags);
      i =
          connectionInterceptor =
              connection ->
                  new MicrometerDuplexConnection(
                      Objects.requireNonNull(connection, "connection"),
                      connectionMeters,
                      inboundFrameMeters,
                      outboundFrameMeters);
    }
    return i;
  }
}
