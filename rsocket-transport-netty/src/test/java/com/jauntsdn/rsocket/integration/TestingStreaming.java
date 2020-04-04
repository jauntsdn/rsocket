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

package com.jauntsdn.rsocket.integration;

import com.jauntsdn.rsocket.AbstractRSocket;
import com.jauntsdn.rsocket.Payload;
import com.jauntsdn.rsocket.RSocketFactory;
import com.jauntsdn.rsocket.exceptions.ApplicationErrorException;
import com.jauntsdn.rsocket.transport.ClientTransport;
import com.jauntsdn.rsocket.transport.ServerTransport;
import com.jauntsdn.rsocket.transport.netty.client.TcpClientTransport;
import com.jauntsdn.rsocket.transport.netty.server.CloseableChannel;
import com.jauntsdn.rsocket.transport.netty.server.TcpServerTransport;
import com.jauntsdn.rsocket.util.DefaultPayload;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TestingStreaming {
  private Supplier<ServerTransport<CloseableChannel>> serverSupplier =
      () -> TcpServerTransport.create("localhost", 0);

  private Function<InetSocketAddress, ClientTransport> clientSupplier = TcpClientTransport::create;

  @Test(expected = ApplicationErrorException.class)
  public void testRangeButThrowException() {
    CloseableChannel server = null;
    try {
      server =
          RSocketFactory.receive()
              .errorConsumer(Throwable::printStackTrace)
              .acceptor(
                  (connectionSetupPayload, rSocket) -> {
                    AbstractRSocket abstractRSocket =
                        new AbstractRSocket() {
                          @Override
                          public double availability() {
                            return 1.0;
                          }

                          @Override
                          public Flux<Payload> requestStream(Payload payload) {
                            return Flux.range(1, 1000)
                                .doOnNext(
                                    i -> {
                                      if (i > 3) {
                                        throw new RuntimeException("BOOM!");
                                      }
                                    })
                                .map(l -> DefaultPayload.create("l -> " + l))
                                .cast(Payload.class);
                          }
                        };

                    return Mono.just(abstractRSocket);
                  })
              .transport(serverSupplier.get())
              .start()
              .block();

      InetSocketAddress address = server.address();
      Flux.range(1, 6).flatMap(i -> consumer(address)).blockLast();
      System.out.println("here");

    } finally {
      if (server != null) {
        server.dispose();
      }
    }
  }

  @Test
  public void testRangeOfConsumers() {
    CloseableChannel server = null;
    try {
      server =
          RSocketFactory.receive()
              .errorConsumer(Throwable::printStackTrace)
              .acceptor(
                  (connectionSetupPayload, rSocket) -> {
                    AbstractRSocket abstractRSocket =
                        new AbstractRSocket() {
                          @Override
                          public double availability() {
                            return 1.0;
                          }

                          @Override
                          public Flux<Payload> requestStream(Payload payload) {
                            return Flux.range(1, 1000)
                                .map(l -> DefaultPayload.create("l -> " + l))
                                .cast(Payload.class);
                          }
                        };

                    return Mono.just(abstractRSocket);
                  })
              .transport(serverSupplier.get())
              .start()
              .block();

      InetSocketAddress address = server.address();
      Flux.range(1, 6).flatMap(i -> consumer(address)).blockLast();
      System.out.println("here");

    } finally {
      if (server != null) {
        server.dispose();
      }
    }
  }

  private Flux<Payload> consumer(InetSocketAddress address) {
    return RSocketFactory.connect()
        .errorConsumer(Throwable::printStackTrace)
        .transport(clientSupplier.apply(address))
        .start()
        .flatMapMany(
            rSocket -> {
              AtomicInteger count = new AtomicInteger();
              return Flux.range(1, 100)
                  .flatMap(
                      i -> rSocket.requestStream(DefaultPayload.create("i -> " + i)).take(100), 1);
            });
  }

  @Test
  public void testSingleConsumer() {
    CloseableChannel server = null;
    try {
      server =
          RSocketFactory.receive()
              .acceptor(
                  (connectionSetupPayload, rSocket) -> {
                    AbstractRSocket abstractRSocket =
                        new AbstractRSocket() {
                          @Override
                          public double availability() {
                            return 1.0;
                          }

                          @Override
                          public Flux<Payload> requestStream(Payload payload) {
                            return Flux.range(1, 10_000)
                                .map(l -> DefaultPayload.create("l -> " + l))
                                .cast(Payload.class);
                          }
                        };

                    return Mono.just(abstractRSocket);
                  })
              .transport(serverSupplier.get())
              .start()
              .block();

      InetSocketAddress address = server.address();
      consumer(address).blockLast();

    } finally {
      if (server != null) {
        server.dispose();
      }
    }
  }

  @Test
  public void testFluxOnly() {
    Flux<Long> longFlux = Flux.interval(Duration.ofMillis(1)).onBackpressureDrop();

    Flux.range(1, 60).flatMap(i -> longFlux.take(1000)).blockLast();
  }
}
