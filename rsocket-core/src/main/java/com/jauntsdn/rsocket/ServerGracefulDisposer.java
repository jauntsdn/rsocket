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
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

interface ServerGracefulDisposer<T extends Closeable> {

  default T ofServer(T t) {
    return t;
  }

  default void start() {}

  default void beforeAccept() {}

  default void afterAccept() {}

  default String tryReject() {
    return null;
  }

  default void onConnect(RSocketRequester rSocket) {}

  @SuppressWarnings("unchecked")
  static <T extends Closeable> ServerGracefulDisposer<T> create(
      ServerGracefulDispose gracefulDispose) {
    if (gracefulDispose.gracefulDisposeOn() != null) {
      return new Impl<>(gracefulDispose);
    } else {
      return (ServerGracefulDisposer<T>) NOOP;
    }
  }

  ServerGracefulDisposer<Closeable> NOOP = new ServerGracefulDisposer<Closeable>() {};

  final class Impl<T extends Closeable> extends AtomicBoolean implements ServerGracefulDisposer<T> {
    private static final Logger logger = LoggerFactory.getLogger(Impl.class);
    private static final String GRACEFULLY_DISPOSED = "gracefully disposed";

    private final Mono<String> disposeSignal;
    private final Duration disposeTimeout;
    private final Set<RSocketRequester> rSockets;

    private final AtomicInteger wip = new AtomicInteger();
    private String disposeMessage;
    private final AtomicInteger rSocketsCount = new AtomicInteger();
    private final MonoProcessor<Void> disposeComplete = MonoProcessor.create();

    Impl(ServerGracefulDispose gracefulDispose) {
      this.disposeSignal = gracefulDispose.gracefulDisposeOn();
      this.disposeTimeout = gracefulDispose.drainTimeout();
      this.rSockets = ConcurrentHashMap.newKeySet();
    }

    @Override
    public void start() {
      disposeSignal.subscribe(
          message -> gracefulDispose(message), err -> gracefulDispose(GRACEFULLY_DISPOSED));
    }

    @Override
    public void beforeAccept() {
      wip.incrementAndGet();
    }

    @Override
    public void afterAccept() {
      int w = wip.decrementAndGet();
      /*last concurrent connection does potential gracefulDispose()*/
      if (w == 1) {
        String m = this.disposeMessage;
        if (m != null) {
          gracefulDispose(m, rSockets);
        }
      }
    }

    @Override
    public String tryReject() {
      return disposeMessage;
    }

    @Override
    public void onConnect(RSocketRequester rSocketRequester) {
      rSockets.add(rSocketRequester);
      rSocketsCount.incrementAndGet();
      rSocketRequester
          .onClose()
          .subscribe(
              v -> {},
              err -> {},
              () -> {
                rSockets.remove(rSocketRequester);
                if (rSocketsCount.decrementAndGet() == 0 && get()) {
                  logger.info("No pending RSockets, server dispose is completed");
                  disposeComplete.onComplete();
                }
              });
    }

    @Override
    public T ofServer(T server) {
      disposeComplete.doOnTerminate(server::dispose).subscribe();
      return server;
    }

    private void gracefulDispose(String m) {
      disposeMessage = m;
      /*no concurrent connection, next will be rejected as there is disposeMessage*/
      if (wip.incrementAndGet() == 1) {
        gracefulDispose(m, rSockets);
        wip.decrementAndGet();
      }
      Duration safeguardTimeout =
          disposeTimeout.plus(Duration.ofMillis(disposeTimeout.toMillis() / 10));
      Mono.delay(safeguardTimeout)
          .subscribe(
              v -> {
                logger.info("Server dispose completed by safeguard timeout");
                disposeComplete.onComplete();
              });
    }

    private void gracefulDispose(String message, Collection<RSocketRequester> rSockets) {
      if (compareAndSet(false, true)) {
        logger.info("Starting server graceful dispose");
        if (rSocketsCount.get() == 0) {
          logger.info("No pending RSockets, server dispose is completed immediately");
          disposeComplete.onComplete();
          return;
        }
        for (RSocketRequester rSocket : rSockets) {
          rSocket.dispose(message, true);
        }
      }
    }
  }
}
