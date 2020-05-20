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

package com.jauntsdn.rsocket.util;

import com.jauntsdn.rsocket.Payload;
import com.jauntsdn.rsocket.RSocket;
import java.util.Optional;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/** Wrapper/Proxy for a RSocket. This is useful when we want to override a specific method. */
public class RSocketProxy implements RSocket {
  protected final RSocket source;

  public RSocketProxy(RSocket source) {
    this.source = source;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return source.fireAndForget(payload);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return source.requestResponse(payload);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return source.requestStream(payload);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return source.requestChannel(payloads);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return source.metadataPush(payload);
  }

  @Override
  public double availability() {
    return source.availability();
  }

  @Override
  public Optional<Scheduler> scheduler() {
    return source.scheduler();
  }

  @Override
  public void dispose() {
    source.dispose();
  }

  @Override
  public void dispose(String reason, boolean isGraceful) {
    source.dispose(reason, isGraceful);
  }

  @Override
  public boolean isDisposed() {
    return source.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return source.onClose();
  }
}
