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

package com.jauntsdn.rsocket;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import com.jauntsdn.rsocket.exceptions.ApplicationErrorException;
import com.jauntsdn.rsocket.frame.decoder.PayloadDecoder;
import com.jauntsdn.rsocket.keepalive.KeepAliveHandler;
import com.jauntsdn.rsocket.test.util.LocalDuplexConnection;
import com.jauntsdn.rsocket.util.DefaultPayload;
import com.jauntsdn.rsocket.util.EmptyPayload;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.time.Duration;
import java.util.ArrayList;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class RSocketTest {

  @Rule public final SocketRule rule = new SocketRule();

  public static void assertError(String s, String mode, ArrayList<Throwable> errors) {
    for (Throwable t : errors) {
      if (t.toString().equals(s)) {
        return;
      }
    }

    Assert.fail("Expected " + mode + " connection error: " + s + " other errors " + errors.size());
  }

  @Test(timeout = 2_000)
  public void testRequestReplyNoError() {
    StepVerifier.create(rule.crs.requestResponse(DefaultPayload.create("hello")))
        .expectNextCount(1)
        .expectComplete()
        .verify();
  }

  @Test(timeout = 2000)
  public void testHandlerEmitsError() {
    rule.setRequestAcceptor(
        new AbstractRSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            return Mono.error(new NullPointerException("Deliberate exception."));
          }
        });
    StepVerifier.create(rule.crs.requestResponse(EmptyPayload.INSTANCE))
        .expectError(ApplicationErrorException.class)
        .verify(Duration.ofMillis(2000));

    // Client sees error through normal API
    rule.assertNoClientErrors();
  }

  @Test(timeout = 2000)
  public void testStream() throws Exception {
    Flux<Payload> responses = rule.crs.requestStream(DefaultPayload.create("Payload In"));
    StepVerifier.create(responses).expectNextCount(10).expectComplete().verify();
  }

  @Test(timeout = 2000)
  public void testChannel() throws Exception {
    Flux<Payload> requests =
        Flux.range(0, 10).map(i -> DefaultPayload.create("streaming in -> " + i));
    Flux<Payload> responses = rule.crs.requestChannel(requests);
    StepVerifier.create(responses).expectNextCount(10).expectComplete().verify();
  }

  public static class SocketRule extends ExternalResource {

    DirectProcessor<ByteBuf> serverProcessor;
    DirectProcessor<ByteBuf> clientProcessor;
    private RSocketRequester crs;

    @SuppressWarnings("unused")
    private RSocketResponder srs;

    private RSocket requestAcceptor;
    private ArrayList<Throwable> clientErrors = new ArrayList<>();
    private ArrayList<Throwable> serverErrors = new ArrayList<>();

    @Override
    public Statement apply(Statement base, Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          init();
          base.evaluate();
        }
      };
    }

    protected void init() {
      serverProcessor = DirectProcessor.create();
      clientProcessor = DirectProcessor.create();

      Scheduler scheduler = Schedulers.single();
      LocalDuplexConnection serverConnection =
          new LocalDuplexConnection("server", clientProcessor, serverProcessor, scheduler);
      LocalDuplexConnection clientConnection =
          new LocalDuplexConnection("client", serverProcessor, clientProcessor, scheduler);

      requestAcceptor =
          null != requestAcceptor
              ? requestAcceptor
              : new AbstractRSocket() {
                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                  return Mono.just(payload);
                }

                @Override
                public Flux<Payload> requestStream(Payload payload) {
                  return Flux.range(1, 10)
                      .map(
                          i -> DefaultPayload.create("server got -> [" + payload.toString() + "]"));
                }

                @Override
                public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                  Flux.from(payloads)
                      .map(
                          payload ->
                              DefaultPayload.create("server got -> [" + payload.toString() + "]"))
                      .subscribe();

                  return Flux.range(1, 10)
                      .map(
                          payload ->
                              DefaultPayload.create("server got -> [" + payload.toString() + "]"));
                }
              };

      srs =
          new RSocketResponder(
              ByteBufAllocator.DEFAULT,
              serverConnection,
              requestAcceptor,
              PayloadDecoder.DEFAULT,
              throwable -> serverErrors.add(throwable),
              StreamErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
              RSocketErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
              100,
              Duration.ofSeconds(1),
              false);

      crs =
          new RSocketRequester(
              ByteBufAllocator.DEFAULT,
              clientConnection,
              PayloadDecoder.DEFAULT,
              throwable -> clientErrors.add(throwable),
              StreamErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
              RSocketErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
              StreamIdSupplier.clientSupplier(),
              100_000,
              100_000,
              new KeepAliveHandler.DefaultKeepAliveHandler(clientConnection),
              Duration.ofSeconds(300),
              false);
    }

    public void setRequestAcceptor(RSocket requestAcceptor) {
      this.requestAcceptor = requestAcceptor;
      init();
    }

    public void assertNoErrors() {
      assertNoClientErrors();
      assertNoServerErrors();
    }

    public void assertNoClientErrors() {
      MatcherAssert.assertThat(
          "Unexpected error on the client connection.", clientErrors, is(empty()));
    }

    public void assertNoServerErrors() {
      MatcherAssert.assertThat(
          "Unexpected error on the server connection.", serverErrors, is(empty()));
    }

    public void assertClientError(String s) {
      assertError(s, "client", this.clientErrors);
    }

    public void assertServerError(String s) {
      assertError(s, "server", this.serverErrors);
    }
  }
}
