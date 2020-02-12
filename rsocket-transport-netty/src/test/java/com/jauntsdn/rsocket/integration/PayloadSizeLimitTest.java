package com.jauntsdn.rsocket.integration;

import static org.junit.Assert.assertTrue;

import com.jauntsdn.rsocket.*;
import com.jauntsdn.rsocket.transport.ClientTransport;
import com.jauntsdn.rsocket.transport.ServerTransport;
import com.jauntsdn.rsocket.transport.netty.server.CloseableChannel;
import com.jauntsdn.rsocket.util.ByteBufPayload;
import com.jauntsdn.rsocket.util.DefaultPayload;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

abstract class PayloadSizeLimitTest {

  private static final int FRAME_SIZE_LIMIT = 1000;
  private CloseableChannel server;
  private final Transport<CloseableChannel> transport;

  PayloadSizeLimitTest(Transport<CloseableChannel> transport) {
    this.transport = transport;
  }

  @BeforeEach
  public void setUp() {
    server =
        RSocketFactory.receive()
            .frameSizeLimit(FRAME_SIZE_LIMIT)
            .acceptor(
                (setup, sendingSocket) ->
                    Mono.just(
                        new AbstractRSocket() {
                          @Override
                          public Flux<Payload> requestStream(Payload payload) {
                            return Flux.just(DefaultPayload.create("RESPONSE", "METADATA"));
                          }
                        }))
            .transport(transport.serverTransport())
            .start()
            .block();
  }

  @AfterEach
  public void tearDown() {
    server.dispose();
    server.onClose().block();
  }

  @Test
  public void testRSocketOnPayloadSizeLimit() {

    RSocket rSocket =
        RSocketFactory.connect()
            .transport(transport.clientTransport().apply(server.address()))
            .start()
            .block();
    Payload request =
        ByteBufPayload.create(request(transport.payloadSizeLimit().apply(FRAME_SIZE_LIMIT)));
    StepVerifier.create(rSocket.requestStream(request))
        .expectNextCount(1)
        .expectComplete()
        .verify(Duration.ofSeconds(5));
  }

  @Test
  public void testRSocketCloseOnPayloadSizeExceeded() {
    RSocket rSocket =
        RSocketFactory.connect()
            .transport(transport.clientTransport.apply(server.address()))
            .start()
            .block();

    Payload request =
        ByteBufPayload.create(request(transport.payloadSizeLimit().apply(FRAME_SIZE_LIMIT) + 1));
    StepVerifier.create(rSocket.requestStream(request)).expectError().verify(Duration.ofSeconds(5));

    assertTrue(rSocket.isDisposed());
  }

  private static byte[] request(int size) {
    byte[] request = new byte[size];
    Arrays.fill(request, (byte) 42);
    return request;
  }

  public static class Transport<T extends Closeable> {
    private final ServerTransport<T> serverTransport;
    private final Function<InetSocketAddress, ClientTransport> clientTransport;
    private final Function<Integer, Integer> payloadSizeLimit;

    public Transport(
        ServerTransport<T> serverTransport,
        Function<InetSocketAddress, ClientTransport> clientTransport,
        Function<Integer, Integer> payloadSizeLimit) {
      this.serverTransport = serverTransport;
      this.clientTransport = clientTransport;
      this.payloadSizeLimit = payloadSizeLimit;
    }

    public ServerTransport<T> serverTransport() {
      return serverTransport;
    }

    public Function<InetSocketAddress, ClientTransport> clientTransport() {
      return clientTransport;
    }

    public Function<Integer, Integer> payloadSizeLimit() {
      return payloadSizeLimit;
    }
  }
}
