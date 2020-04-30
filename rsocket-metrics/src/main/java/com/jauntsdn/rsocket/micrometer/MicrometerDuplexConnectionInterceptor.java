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

import com.jauntsdn.rsocket.interceptors.DuplexConnectionInterceptor;

/**
 * Marker interface for Micrometer DuplexConnection interceptors.
 *
 * @see <a href="https://micrometer.io">Micrometer</a>
 */
@FunctionalInterface
public interface MicrometerDuplexConnectionInterceptor extends DuplexConnectionInterceptor {
  /** Total frames size in bytes. Tagged with {@link #TAG_FRAME_DIRECTION} */
  String COUNTER_FRAMES_SIZE = "rsocket.frame.size.bytes";

  /** Frames count. Tagged with {@link #TAG_FRAME_DIRECTION} */
  String COUNTER_FRAMES_COUNT = "rsocket.frame";

  /** Number of opened connections. Tagged with {@link #TAG_FRAME_DIRECTION} */
  String COUNTER_CONNECTIONS_OPENED = "rsocket.connection.opened";

  /** Number of closed connections. Tagged with {@link #TAG_FRAME_DIRECTION} */
  String COUNTER_CONNECTIONS_CLOSED = "rsocket.connection.closed";

  /** Frames direction for one side of connection */
  String TAG_FRAME_DIRECTION = "direction";

  String TAG_FRAME_DIRECTION_INBOUND = "inbound";
  String TAG_FRAME_DIRECTION_OUTBOUND = "outbound";
}
