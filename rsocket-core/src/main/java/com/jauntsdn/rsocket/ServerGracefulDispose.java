package com.jauntsdn.rsocket;

import java.time.Duration;
import java.util.Objects;
import reactor.core.publisher.Mono;

/**
 * Gracefully disposed RSocket sends 0 error frame with {@link
 * com.jauntsdn.rsocket.frame.ErrorCodes#CONNECTION_CLOSE}, and is terminated after drain duration.
 * Responder RSocket leases are stopped. RSocket requester receiving graceful shutdown reports
 * itself as unavailable (availability = 0.0).
 *
 * <p>Gracefully disposed server stops accepting new connections with either {@link
 * com.jauntsdn.rsocket.frame.ErrorCodes#CONNECTION_CLOSE} or {@link
 * com.jauntsdn.rsocket.frame.ErrorCodes#REJECTED_RESUME} code, and gracefully disposes all its
 * RSockets. Once all RSockets are closed after drain timeout, server is disposed.
 */
public final class ServerGracefulDispose {
  private Duration drainTimeout;
  private Mono<String> disposeSignal;

  ServerGracefulDispose() {}

  static ServerGracefulDispose create() {
    return new ServerGracefulDispose();
  }

  /**
   * @param drainTimeout drain timeout between individual RSocket or server graceful disposal, and
   *     actual shutdown.
   * @return this {@link ServerGracefulDispose} instance
   */
  public ServerGracefulDispose drainTimeout(Duration drainTimeout) {
    this.drainTimeout = Objects.requireNonNull(drainTimeout, "drainTimeout");
    return this;
  }

  /**
   * @param disposeSignal signal completion triggers graceful dispose of server.
   * @return this {@link ServerGracefulDispose} instance
   */
  public ServerGracefulDispose gracefulDisposeOn(Mono<String> disposeSignal) {
    this.disposeSignal = Objects.requireNonNull(disposeSignal, "disposeSignal");
    return this;
  }

  public Duration drainTimeout() {
    return drainTimeout;
  }

  public Mono<String> gracefulDisposeOn() {
    return disposeSignal;
  }

  public interface Configurer {

    void configure(ServerGracefulDispose gracefulDispose);
  }
}
