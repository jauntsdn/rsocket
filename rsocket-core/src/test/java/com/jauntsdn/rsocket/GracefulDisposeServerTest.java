package com.jauntsdn.rsocket;

import com.jauntsdn.rsocket.frame.*;
import com.jauntsdn.rsocket.test.util.TestDuplexConnection;
import com.jauntsdn.rsocket.test.util.TestServerTransport;
import com.jauntsdn.rsocket.util.EmptyPayload;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.time.Duration;
import java.util.Collection;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class GracefulDisposeServerTest {

  @Test
  void serverDispose() {
    String disposeMessage = "dispose";
    int drainMillis = 500;

    MonoProcessor<String> disposeOn = MonoProcessor.create();
    MonoProcessor<String> connected = MonoProcessor.create();

    TestServerTransport serverTransport = new TestServerTransport();
    Closeable server =
        RSocketFactory.receive()
            .gracefulDispose(
                c -> c.drainTimeout(Duration.ofMillis(drainMillis)).gracefulDisposeOn(disposeOn))
            .acceptor(
                (setup, sendingSocket) -> {
                  Mono<RSocket> handler = Mono.just(new AbstractRSocket() {});
                  connected.onComplete();
                  return handler;
                })
            .transport(serverTransport)
            .start()
            .block();

    TestDuplexConnection connection = serverTransport.connect(Schedulers.single());
    connection.addToReceivedBuffer(
        SetupFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, false, 10_000, 1000_000, "", "", EmptyPayload.INSTANCE));

    connected.block(Duration.ofSeconds(1));

    long start = System.currentTimeMillis();
    disposeOn.onNext(disposeMessage);

    Collection<ByteBuf> sent = connection.getSent();
    Assertions.assertThat(sent).hasSize(1);

    ByteBuf frame = sent.iterator().next();

    Assertions.assertThat(FrameHeaderFlyweight.frameType(frame)).isEqualTo(FrameType.ERROR);

    int errorCode = ErrorFrameFlyweight.errorCode(frame);
    String message = ErrorFrameFlyweight.dataUtf8(frame);
    Assertions.assertThat(errorCode).isEqualTo(ErrorCodes.CONNECTION_CLOSE);
    Assertions.assertThat(message).isEqualTo(disposeMessage);

    server.onClose().as(StepVerifier::create).expectComplete().verify(Duration.ofSeconds(1));
    Assertions.assertThat(System.currentTimeMillis() - start)
        .isCloseTo(drainMillis, Offset.offset(50L));
    Assertions.assertThat(connection.isDisposed());
  }
}
