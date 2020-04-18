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

import com.jauntsdn.rsocket.DuplexConnection;
import com.jauntsdn.rsocket.plugins.DuplexConnectionInterceptor;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.util.Objects;

/**
 * An implementation of {@link DuplexConnectionInterceptor} that intercepts frames and gathers
 * Micrometer metrics about them.
 *
 * @see <a href="https://micrometer.io">Micrometer</a>
 */
public final class MicrometerDuplexConnectionInterceptor implements DuplexConnectionInterceptor {

  private final MeterRegistry meterRegistry;

  private final Tag[] tags;

  /**
   * Creates a new {@link DuplexConnectionInterceptor}.
   *
   * @param meterRegistry the {@link MeterRegistry} to use to create {@link Meter}s.
   * @param tags the additional tags to attach to each {@link Meter}
   * @throws NullPointerException if {@code meterRegistry} is {@code null}
   */
  public MicrometerDuplexConnectionInterceptor(MeterRegistry meterRegistry, Tag... tags) {
    this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
    this.tags = tags;
  }

  @Override
  public MicrometerDuplexConnection apply(DuplexConnection delegate) {
    Objects.requireNonNull(delegate, "delegate must not be null");

    return new MicrometerDuplexConnection(delegate, meterRegistry, tags);
  }
}
