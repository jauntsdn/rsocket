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

package com.jauntsdn.rsocket;

import java.time.Duration;
import java.util.Objects;

/**
 * Gracefully disposed RSocket sends 0 error frame with {@link
 * com.jauntsdn.rsocket.frame.ErrorCodes#CONNECTION_CLOSE}, and is terminated after drain duration.
 * Responder RSocket leases are stopped. RSocket requester receiving graceful shutdown reports
 * itself as unavailable (availability = 0.0).
 */
public final class ClientGracefulDispose {
  private Duration drainTimeout;

  ClientGracefulDispose() {}

  static ClientGracefulDispose create() {
    return new ClientGracefulDispose();
  }

  /**
   * @param drainTimeout drain timeout between individual RSocket graceful disposal, and actual
   *     shutdown.
   * @return this {@link ClientGracefulDispose} instance
   */
  public ClientGracefulDispose drainTimeout(Duration drainTimeout) {
    this.drainTimeout = Objects.requireNonNull(drainTimeout, "drainTimeout");
    return this;
  }

  public Duration drainTimeout() {
    return drainTimeout;
  }

  public interface Configurer {

    void configure(ClientGracefulDispose gracefulDispose);
  }
}
