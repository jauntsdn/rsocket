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

package com.jauntsdn.rsocket.test.util;

import com.jauntsdn.rsocket.DuplexConnection;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class LocalDuplexConnection implements DuplexConnection {
  private final DirectProcessor<ByteBuf> send;
  private final DirectProcessor<ByteBuf> receive;
  private final Scheduler scheduler;
  private final MonoProcessor<Void> onClose;
  private final String name;

  public LocalDuplexConnection(
      String name, DirectProcessor<ByteBuf> send, DirectProcessor<ByteBuf> receive) {
    this(name, send, receive, ConnectionScheduler.DEFAULT);
  }

  public LocalDuplexConnection(
      String name,
      DirectProcessor<ByteBuf> send,
      DirectProcessor<ByteBuf> receive,
      Scheduler scheduler) {
    this.name = name;
    this.send = send;
    this.receive = receive;
    onClose = MonoProcessor.create();
    this.scheduler = scheduler;
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frame) {
    return Flux.from(frame)
        .doOnNext(f -> System.out.println(name + " - " + f.toString()))
        .doOnNext(send::onNext)
        .doOnError(send::onError)
        .then();
  }

  @Override
  public Flux<ByteBuf> receive() {
    return receive.doOnNext(f -> System.out.println(name + " - " + f.toString()));
  }

  @Override
  public Scheduler scheduler() {
    return scheduler;
  }

  @Override
  public void dispose() {
    onClose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  private static class ConnectionScheduler {
    static final Scheduler DEFAULT = Schedulers.immediate();
  }
}
