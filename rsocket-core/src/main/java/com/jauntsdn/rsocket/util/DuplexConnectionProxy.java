/*
 * Copyright 2015-2019 the original author or authors.
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

package com.jauntsdn.rsocket.util;

import com.jauntsdn.rsocket.DuplexConnection;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class DuplexConnectionProxy implements DuplexConnection {
  private final DuplexConnection connection;

  public DuplexConnectionProxy(DuplexConnection connection) {
    this.connection = connection;
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    return connection.send(frames);
  }

  @Override
  public Flux<ByteBuf> receive() {
    return connection.receive();
  }

  @Override
  public double availability() {
    return connection.availability();
  }

  @Override
  public Scheduler scheduler() {
    return connection.scheduler();
  }

  @Override
  public Mono<Void> onClose() {
    return connection.onClose();
  }

  @Override
  public void dispose() {
    connection.dispose();
  }

  @Override
  public boolean isDisposed() {
    return connection.isDisposed();
  }

  public DuplexConnection delegate() {
    return connection;
  }
}
