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

package com.jauntsdn.rsocket.test;

import com.jauntsdn.rsocket.AbstractRSocket;
import com.jauntsdn.rsocket.ConnectionSetupPayload;
import com.jauntsdn.rsocket.Payload;
import com.jauntsdn.rsocket.RSocket;
import com.jauntsdn.rsocket.ServerSocketAcceptor;
import com.jauntsdn.rsocket.util.ByteBufPayload;
import java.util.concurrent.ThreadLocalRandom;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class PingHandler implements ServerSocketAcceptor {

  private final Payload pong;

  public PingHandler() {
    byte[] data = new byte[1024];
    ThreadLocalRandom.current().nextBytes(data);
    pong = ByteBufPayload.create(data);
  }

  public PingHandler(byte[] data) {
    pong = ByteBufPayload.create(data);
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
    return Mono.just(
        new AbstractRSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            payload.release();
            return Mono.just(pong.retain());
          }

          @Override
          public Flux<Payload> requestStream(Payload payload) {
            payload.release();
            return Flux.range(0, 100).map(v -> pong.retain());
          }
        });
  }
}
