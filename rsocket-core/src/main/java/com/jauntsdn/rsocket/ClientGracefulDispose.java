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
