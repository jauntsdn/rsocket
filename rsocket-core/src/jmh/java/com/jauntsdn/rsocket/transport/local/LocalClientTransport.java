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

package com.jauntsdn.rsocket.transport.local;

import com.jauntsdn.rsocket.DuplexConnection;
import com.jauntsdn.rsocket.internal.UnboundedProcessor;
import com.jauntsdn.rsocket.transport.ClientTransport;
import com.jauntsdn.rsocket.transport.ServerTransport;
import io.netty.buffer.ByteBuf;
import java.util.Objects;
import javax.annotation.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;

/**
 * An implementation of {@link ClientTransport} that connects to a {@link ServerTransport} in the
 * same JVM.
 */
public final class LocalClientTransport implements ClientTransport {

  private final String name;
  private final Scheduler scheduler;

  private LocalClientTransport(String name, @Nullable Scheduler scheduler) {
    this.name = name;
    this.scheduler = scheduler;
  }

  /**
   * Creates a new instance.
   *
   * @param name the name of the {@link ServerTransport} instance to connect to
   * @return a new instance
   * @throws NullPointerException if {@code name} is {@code null}
   */
  public static LocalClientTransport create(String name) {
    return new LocalClientTransport(Objects.requireNonNull(name, "name must not be null"), null);
  }

  /**
   * Creates a new instance.
   *
   * @param name the name of the {@link ServerTransport} instance to connect to
   * @return a new instance
   * @throws NullPointerException if {@code name} is {@code null}
   */
  public static LocalClientTransport create(String name, Scheduler scheduler) {
    return new LocalClientTransport(
        Objects.requireNonNull(name, "name must not be null"), scheduler);
  }

  @Override
  public Mono<DuplexConnection> connect(int frameSizeLimit) {
    return Mono.defer(
        () -> {
          LocalServerTransport.ServerDuplexConnectionAcceptor server =
              LocalServerTransport.findServer(name);
          if (server == null) {
            return Mono.error(new IllegalArgumentException("Could not find server: " + name));
          }

          UnboundedProcessor<ByteBuf> in = new UnboundedProcessor<>();
          UnboundedProcessor<ByteBuf> out = new UnboundedProcessor<>();
          MonoProcessor<Void> closeNotifier = MonoProcessor.create();
          Scheduler scheduler = this.scheduler;
          if (scheduler != null) {
            server.accept(new LocalDuplexConnection(out, in, closeNotifier, scheduler));
            return Mono.just(new LocalDuplexConnection(in, out, closeNotifier, scheduler));
          } else {
            server.accept(new LocalDuplexConnection(out, in, closeNotifier));
            return Mono.just(new LocalDuplexConnection(in, out, closeNotifier));
          }
        });
  }
}
