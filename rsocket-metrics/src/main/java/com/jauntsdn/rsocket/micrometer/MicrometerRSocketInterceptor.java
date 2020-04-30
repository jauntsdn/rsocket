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

import com.jauntsdn.rsocket.interceptors.RSocketInterceptor;

/**
 * Marker interface for Micrometer RSocket interceptors.
 *
 * @see <a href="https://micrometer.io">Micrometer</a>
 */
@FunctionalInterface
public interface MicrometerRSocketInterceptor extends RSocketInterceptor {
  /**
   * RSocket started requests count. Tagged with {@link #TAG_INTERACTION_TYPE and
   * {@link #TAG_RSOCKET_ROLE}
   */
  String COUNTER_RSOCKET_REQUEST_STARTED = "rsocket.request.started";

  /**
   * RSocket completed requests count. Tagged with {@link #TAG_INTERACTION_TYPE}, {@link
   * #TAG_SIGNAL_TYPE} and {@link #TAG_RSOCKET_ROLE}
   */
  String COUNTER_RSOCKET_REQUEST_COMPLETED = "rsocket.request.completed";

  /** RSocket interaction lifecycle signals */
  String TAG_INTERACTION_TYPE = "interaction";

  String TAG_INTERACTION_TYPE_METADATA_PUSH = "metadatapush";
  String TAG_INTERACTION_TYPE_FNF = "fnf";
  String TAG_INTERACTION_TYPE_RESPONSE = "response";
  String TAG_INTERACTION_TYPE_STREAM = "stream";
  String TAG_INTERACTION_TYPE_CHANNEL = "channel";

  /** RSocket interaction completion signal */
  String TAG_SIGNAL_TYPE = "signal";

  String TAG_SIGNAL_TYPE_COMPLETE = "complete";
  String TAG_SIGNAL_TYPE_ERROR = "error";
  String TAG_SIGNAL_TYPE_CANCEL = "cancel";

  /** RSocket role for one side of connection */
  String TAG_RSOCKET_ROLE = "role";

  String TAG_RSOCKET_ROLE_REQUESTER = "requester";
  String TAG_RSOCKET_ROLE_RESPONDER = "responder";
}
