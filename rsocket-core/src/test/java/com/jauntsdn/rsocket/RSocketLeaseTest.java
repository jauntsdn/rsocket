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

package com.jauntsdn.rsocket;

import static com.jauntsdn.rsocket.frame.FrameType.ERROR;
import static com.jauntsdn.rsocket.frame.FrameType.SETUP;
import static org.assertj.core.data.Offset.offset;
import static org.mockito.Mockito.*;

import com.jauntsdn.rsocket.exceptions.Exceptions;
import com.jauntsdn.rsocket.exceptions.RejectedException;
import com.jauntsdn.rsocket.frame.FrameHeaderFlyweight;
import com.jauntsdn.rsocket.frame.FrameType;
import com.jauntsdn.rsocket.frame.LeaseFrameFlyweight;
import com.jauntsdn.rsocket.frame.SetupFrameFlyweight;
import com.jauntsdn.rsocket.frame.decoder.PayloadDecoder;
import com.jauntsdn.rsocket.internal.ClientServerInputMultiplexer;
import com.jauntsdn.rsocket.keepalive.KeepAliveHandler;
import com.jauntsdn.rsocket.lease.Lease;
import com.jauntsdn.rsocket.lease.RequesterLeaseHandler;
import com.jauntsdn.rsocket.lease.ResponderLeaseHandler;
import com.jauntsdn.rsocket.plugins.PluginRegistry;
import com.jauntsdn.rsocket.test.util.TestClientTransport;
import com.jauntsdn.rsocket.test.util.TestDuplexConnection;
import com.jauntsdn.rsocket.test.util.TestServerTransport;
import com.jauntsdn.rsocket.util.DefaultPayload;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

class RSocketLeaseTest {
  private RSocketRequester rSocketRequester;
  private ResponderLeaseHandler responderLeaseHandler;
  private ByteBufAllocator byteBufAllocator;
  private TestDuplexConnection connection;
  private RSocketResponder rSocketResponder;

  private EmitterProcessor<Lease> leaseSender = EmitterProcessor.create();
  private RequesterLeaseHandler requesterLeaseHandler;

  @BeforeEach
  void setUp() {
    connection = new TestDuplexConnection(Schedulers.single());
    PayloadDecoder payloadDecoder = PayloadDecoder.DEFAULT;
    byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;

    requesterLeaseHandler = new RequesterLeaseHandler.Impl();
    responderLeaseHandler =
        new ResponderLeaseHandler.Impl<>(
            byteBufAllocator, stats -> leaseSender, err -> {}, Optional.empty());

    ClientServerInputMultiplexer multiplexer =
        new ClientServerInputMultiplexer(connection, new PluginRegistry(), true);
    rSocketRequester =
        new LeaseRSocketRequester(
            byteBufAllocator,
            multiplexer.asClientConnection(),
            payloadDecoder,
            err -> {},
            StreamErrorMappers.create().createErrorFrameMapper(ByteBufAllocator.DEFAULT),
            StreamIdSupplier.clientSupplier(),
            100_000,
            100_000,
            new KeepAliveHandler.DefaultKeepAliveHandler(connection),
            requesterLeaseHandler,
            rtt -> {});

    RSocket mockRSocketHandler = mock(RSocket.class);
    when(mockRSocketHandler.metadataPush(any())).thenReturn(Mono.empty());
    when(mockRSocketHandler.fireAndForget(any())).thenReturn(Mono.empty());
    when(mockRSocketHandler.requestResponse(any())).thenReturn(Mono.empty());
    when(mockRSocketHandler.requestStream(any())).thenReturn(Flux.empty());
    when(mockRSocketHandler.requestChannel(any())).thenReturn(Flux.empty());

    rSocketResponder =
        new LeaseRSocketResponder(
            byteBufAllocator,
            multiplexer.asServerConnection(),
            mockRSocketHandler,
            payloadDecoder,
            err -> {},
            StreamErrorMappers.create().createErrorFrameMapper(ByteBufAllocator.DEFAULT),
            responderLeaseHandler);
  }

  @Test
  public void serverRSocketFactoryRejectsUnsupportedLease() {
    Payload payload = DefaultPayload.create(DefaultPayload.EMPTY_BUFFER);
    ByteBuf setupFrame =
        SetupFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT,
            true,
            1000,
            30_000,
            "application/octet-stream",
            "application/octet-stream",
            payload);

    TestServerTransport transport = new TestServerTransport();
    Closeable server =
        RSocketFactory.receive()
            .acceptor((setup, sendingSocket) -> Mono.just(new AbstractRSocket() {}))
            .transport(transport)
            .start()
            .block();

    TestDuplexConnection connection = transport.connect();
    connection.addToReceivedBuffer(setupFrame);

    Collection<ByteBuf> sent = connection.getSent();
    Assertions.assertThat(sent).hasSize(1);
    ByteBuf error = sent.iterator().next();
    Assertions.assertThat(FrameHeaderFlyweight.frameType(error)).isEqualTo(ERROR);
    Assertions.assertThat(Exceptions.from(error).getMessage()).isEqualTo("lease is not supported");
  }

  @Test
  public void clientRSocketFactorySetsLeaseFlag() {
    TestClientTransport clientTransport = new TestClientTransport(Schedulers.single());
    RSocketFactory.connect()
        .lease(((rtt, scheduler) -> Leases.create()))
        .transport(clientTransport)
        .start()
        .block();

    Collection<ByteBuf> sent = clientTransport.testConnection().getSent();
    Assertions.assertThat(sent).hasSize(1);
    ByteBuf setup = sent.iterator().next();
    Assertions.assertThat(FrameHeaderFlyweight.frameType(setup)).isEqualTo(SETUP);
    Assertions.assertThat(SetupFrameFlyweight.honorLease(setup)).isTrue();
  }

  @Test
  void requesterInitialLeaseIsZero() {
    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.0, offset(1e-2));
  }

  @ParameterizedTest
  @MethodSource("requesterInteractions")
  void requesterPresentLeaseRequestsAreAccepted(
      Function<RSocketRequester, Publisher<?>> interaction) {
    requesterLeaseHandler.receive(leaseFrame(5_000, 2, Unpooled.EMPTY_BUFFER));

    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(1.0, offset(1e-2));

    Flux.from(interaction.apply(rSocketRequester))
        .take(Duration.ofMillis(500))
        .as(StepVerifier::create)
        .expectComplete()
        .verify(Duration.ofSeconds(5));
    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.5, offset(1e-2));
  }

  @Test
  void requesterAvailabilityRespectsTransport() {
    requesterLeaseHandler.receive(leaseFrame(5_000, 1, Unpooled.EMPTY_BUFFER));
    double unavailable = 0.0;
    connection.setAvailability(unavailable);
    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(unavailable, offset(1e-2));
  }

  @ParameterizedTest
  @MethodSource("responderInteractions")
  void responderMissingLeaseRequestsAreRejected(
      Function<RSocketResponder, Publisher<?>> interaction) {
    StepVerifier.create(interaction.apply(rSocketResponder))
        .expectError(RejectedException.class)
        .verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("responderInteractions")
  void responderPresentLeaseRequestsAreAccepted(
      Function<RSocketResponder, Publisher<?>> interaction) {
    leaseSender.onNext(Lease.create(5_000, 2));

    Flux.from(interaction.apply(rSocketResponder))
        .take(Duration.ofMillis(500))
        .as(StepVerifier::create)
        .expectComplete()
        .verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("responderInteractions")
  void responderDepletedAllowedLeaseRequestsAreRejected(
      Function<RSocketResponder, Publisher<?>> interaction) {
    leaseSender.onNext(Lease.create(5_000, 1));

    Flux<?> responder = Flux.from(interaction.apply(rSocketResponder));
    responder.subscribe();
    Flux.from(interaction.apply(rSocketResponder))
        .as(StepVerifier::create)
        .expectError(RejectedException.class)
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void sendLease() {
    Charset utf8 = StandardCharsets.UTF_8;
    int ttl = 5_000;
    int numberOfRequests = 2;
    leaseSender.onNext(Lease.create(5_000, 2));

    ByteBuf leaseFrame =
        connection
            .getSent()
            .stream()
            .filter(f -> FrameHeaderFlyweight.frameType(f) == FrameType.LEASE)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Lease frame not sent"));

    Assertions.assertThat(LeaseFrameFlyweight.ttl(leaseFrame)).isEqualTo(ttl);
    Assertions.assertThat(LeaseFrameFlyweight.numRequests(leaseFrame)).isEqualTo(numberOfRequests);
  }

  ByteBuf leaseFrame(int ttl, int requests, ByteBuf metadata) {
    return LeaseFrameFlyweight.encode(byteBufAllocator, ttl, requests, metadata);
  }

  static Stream<Function<RSocketRequester, Publisher<?>>> requesterInteractions() {
    return Stream.of(
        rSocket -> rSocket.fireAndForget(DefaultPayload.create("test")),
        rSocket -> rSocket.requestResponse(DefaultPayload.create("test")),
        rSocket -> rSocket.requestStream(DefaultPayload.create("test")),
        rSocket -> rSocket.requestChannel(Mono.just(DefaultPayload.create("test"))));
  }

  static Stream<Function<ResponderRSocket, Publisher<?>>> responderInteractions() {
    return Stream.of(
        rSocket -> rSocket.fireAndForget(DefaultPayload.create("test")),
        rSocket -> rSocket.requestResponse(DefaultPayload.create("test")),
        rSocket -> rSocket.requestStream(DefaultPayload.create("test")),
        rSocket -> {
          Payload request = DefaultPayload.create("test");
          return rSocket.requestChannel(request, Mono.just(request));
        });
  }
}
