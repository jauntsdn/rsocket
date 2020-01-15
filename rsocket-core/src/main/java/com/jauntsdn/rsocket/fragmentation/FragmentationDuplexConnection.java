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

package com.jauntsdn.rsocket.fragmentation;

import com.jauntsdn.rsocket.DuplexConnection;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Objects;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * A {@link DuplexConnection} implementation that reassembles {@link ByteBuf}s.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#fragmentation-and-reassembly">Fragmentation
 *     and Reassembly</a>
 */
public final class FragmentationDuplexConnection implements DuplexConnection {
  private static final Logger logger = LoggerFactory.getLogger(FragmentationDuplexConnection.class);
  private final DuplexConnection delegate;
  private final FrameReassembler frameReassembler;

  public FragmentationDuplexConnection(DuplexConnection delegate, ByteBufAllocator allocator) {
    Objects.requireNonNull(delegate, "delegate must not be null");
    Objects.requireNonNull(allocator, "byteBufAllocator must not be null");
    this.delegate = delegate;
    this.frameReassembler = new FrameReassembler(allocator);

    delegate.onClose().doFinally(s -> frameReassembler.dispose()).subscribe();
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    return delegate.send(frames);
  }

  @Override
  public Mono<Void> sendOne(ByteBuf frame) {
    return delegate.sendOne(frame);
  }

  @Override
  public Flux<ByteBuf> receive() {
    return delegate.receive().handle(frameReassembler::reassembleFrame);
  }

  @Override
  public Scheduler scheduler() {
    return delegate.scheduler();
  }

  @Override
  public Mono<Void> onClose() {
    return delegate.onClose();
  }

  @Override
  public void dispose() {
    delegate.dispose();
  }
}
