/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jauntsdn.rsocket.transport.netty;

import com.jauntsdn.rsocket.DuplexConnection;
import com.jauntsdn.rsocket.frame.FrameLengthFlyweight;
import com.jauntsdn.rsocket.internal.BaseDuplexConnection;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Objects;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.Connection;

/** An implementation of {@link DuplexConnection} that connects via TCP. */
public final class TcpDuplexConnection extends BaseDuplexConnection {

  private final Connection connection;
  private final ByteBufAllocator allocator;
  private final Scheduler scheduler;

  /**
   * Creates a new instance
   *
   * @param connection the {@link Connection} to for managing the server
   */
  public TcpDuplexConnection(Connection connection) {
    this.connection = Objects.requireNonNull(connection, "connection must not be null");
    this.scheduler = Schedulers.fromExecutor(connection.channel().eventLoop());
    this.allocator = connection.channel().alloc();

    connection
        .channel()
        .closeFuture()
        .addListener(
            future -> {
              if (!isDisposed()) dispose();
            });
  }

  @Override
  protected void doOnClose() {
    if (!connection.isDisposed()) {
      connection.dispose();
    }
  }

  @Override
  public Flux<ByteBuf> receive() {
    return connection.inbound().receive().map(this::decode);
  }

  @Override
  public Scheduler scheduler() {
    return scheduler;
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    if (frames instanceof Mono) {
      return connection.outbound().sendObject(((Mono<ByteBuf>) frames).map(this::encode)).then();
    }
    return connection.outbound().send(Flux.from(frames).map(this::encode)).then();
  }

  private ByteBuf encode(ByteBuf frame) {
    return FrameLengthFlyweight.encode(allocator, frame.readableBytes(), frame);
  }

  private ByteBuf decode(ByteBuf frame) {
    return FrameLengthFlyweight.frame(frame).retain();
  }
}
