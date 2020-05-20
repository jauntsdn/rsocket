package com.jauntsdn.rsocket;

import com.jauntsdn.rsocket.exceptions.ConnectionCloseException;
import com.jauntsdn.rsocket.frame.ErrorCodes;
import com.jauntsdn.rsocket.frame.ErrorFrameFlyweight;
import com.jauntsdn.rsocket.frame.FrameHeaderFlyweight;
import com.jauntsdn.rsocket.frame.FrameType;
import com.jauntsdn.rsocket.test.util.TestClientTransport;
import com.jauntsdn.rsocket.test.util.TestDuplexConnection;
import com.jauntsdn.rsocket.util.DefaultPayload;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class GracefulDisposeTest {

  @Test
  void requesterDisposeLocal() {
    TestClientTransport transport = new TestClientTransport(Schedulers.single());
    int drainTimeout = 500;
    String disposeMessage = "graceful";

    RSocket rSocket =
        RSocketFactory.connect()
            .gracefulDispose(c -> c.drainTimeout(Duration.ofMillis(drainTimeout)))
            .transport(transport)
            .start()
            .block();

    long start = System.currentTimeMillis();
    rSocket.dispose(disposeMessage, true);

    rSocket.onClose().as(StepVerifier::create).expectComplete().verify(Duration.ofSeconds(1));

    Assertions.assertThat(System.currentTimeMillis() - start)
        .isCloseTo(drainTimeout, Offset.offset(100L));

    rSocket
        .requestResponse(DefaultPayload.create("test"))
        .as(StepVerifier::create)
        .expectErrorMatches(
            err ->
                err instanceof ConnectionCloseException && disposeMessage.equals(err.getMessage()))
        .verify(Duration.ofSeconds(1));

    TestDuplexConnection c = transport.testConnection();
    Collection<ByteBuf> sentFrames = c.getSent();
    Assertions.assertThat(sentFrames).hasSize(2);
    Iterator<ByteBuf> iterator = sentFrames.iterator();
    ByteBuf setupFrame = iterator.next();
    Assertions.assertThat(FrameHeaderFlyweight.frameType(setupFrame)).isEqualTo(FrameType.SETUP);
    ByteBuf closeFrame = iterator.next();

    Assertions.assertThat(FrameHeaderFlyweight.frameType(closeFrame)).isEqualTo(FrameType.ERROR);
    Assertions.assertThat(ErrorFrameFlyweight.errorCode(closeFrame))
        .isEqualTo(ErrorCodes.CONNECTION_CLOSE);
    Assertions.assertThat(ErrorFrameFlyweight.dataUtf8(closeFrame)).isEqualTo(disposeMessage);

    setupFrame.release();
    closeFrame.release();
  }

  @Test
  void requesterDisposeLocalAfterGracefulDispose() {
    TestClientTransport transport = new TestClientTransport(Schedulers.single());
    String disposeMessage = "graceful";

    RSocket rSocket =
        RSocketFactory.connect()
            .gracefulDispose(c -> c.drainTimeout(Duration.ofSeconds(42)))
            .transport(transport)
            .start()
            .block();

    rSocket.dispose(disposeMessage, true);

    Mono.delay(Duration.ofMillis(100)).subscribe(v -> transport.testConnection().dispose());

    rSocket.onClose().as(StepVerifier::create).expectComplete().verify(Duration.ofSeconds(1));

    rSocket
        .requestResponse(DefaultPayload.create("test"))
        .as(StepVerifier::create)
        .expectErrorMatches(err -> err instanceof ClosedChannelException)
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void requesterDisposeRemote() {
    TestClientTransport transport = new TestClientTransport(Schedulers.single());
    RSocket rSocket = RSocketFactory.connect().transport(transport).start().block();

    TestDuplexConnection c = transport.testConnection();
    String disposeMessage = "dispose";
    c.addToReceivedBuffer(
        ErrorFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, 0, ErrorCodes.CONNECTION_CLOSE, disposeMessage));

    Assertions.assertThat(rSocket.availability()).isCloseTo(0.0, Offset.offset(1e-5));
    Assertions.assertThat(rSocket.isDisposed()).isTrue();

    c.dispose();

    rSocket.onClose().as(StepVerifier::create).expectComplete().verify(Duration.ofSeconds(5));

    rSocket
        .requestResponse(DefaultPayload.create("test"))
        .as(StepVerifier::create)
        .expectErrorMatches(
            err ->
                disposeMessage.equals(err.getMessage()) && err instanceof ConnectionCloseException)
        .verify(Duration.ofSeconds(5));
  }
}
