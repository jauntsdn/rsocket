package com.jauntsdn.rsocket;

import static com.jauntsdn.rsocket.transport.ServerTransport.ConnectionAcceptor;
import static org.assertj.core.api.Assertions.assertThat;

import com.jauntsdn.rsocket.exceptions.ConnectionErrorException;
import com.jauntsdn.rsocket.exceptions.Exceptions;
import com.jauntsdn.rsocket.exceptions.RejectedSetupException;
import com.jauntsdn.rsocket.frame.*;
import com.jauntsdn.rsocket.frame.decoder.PayloadDecoder;
import com.jauntsdn.rsocket.keepalive.KeepAliveHandler;
import com.jauntsdn.rsocket.test.util.TestClientTransport;
import com.jauntsdn.rsocket.test.util.TestDuplexConnection;
import com.jauntsdn.rsocket.transport.ServerTransport;
import com.jauntsdn.rsocket.util.ByteBufPayload;
import com.jauntsdn.rsocket.util.DefaultPayload;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class RSocketConnectionTerminationTest {

  @Test
  void serverRejectSetup() {
    SingleConnectionTransport transport = new SingleConnectionTransport();

    String errorMsg = "error";
    RejectingAcceptor acceptor = new RejectingAcceptor(errorMsg);
    RSocketFactory.receive().acceptor(acceptor).transport(transport).start().block();

    transport.connect();

    ByteBuf sentFrame = transport.awaitSent();
    assertThat(FrameHeaderFlyweight.frameType(sentFrame)).isEqualTo(FrameType.ERROR);
    RuntimeException error = Exceptions.from(sentFrame);
    assertThat(errorMsg).isEqualTo(error.getMessage());
    assertThat(error).isInstanceOf(RejectedSetupException.class);
    RSocket acceptorSender = acceptor.senderRSocket().block();
    assertThat(acceptorSender.isDisposed()).isTrue();
  }

  @Test
  void clientRejectSetup() {
    TestDuplexConnection conn = new TestDuplexConnection(Schedulers.single());
    List<Throwable> errors = new ArrayList<>();
    RSocketRequester rSocket =
        new RSocketRequester(
            ByteBufAllocator.DEFAULT,
            conn,
            PayloadDecoder.DEFAULT,
            errors::add,
            StreamErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
            RSocketErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
            StreamIdSupplier.clientSupplier(),
            100_000,
            100_000,
            new KeepAliveHandler.DefaultKeepAliveHandler(conn),
            Duration.ofSeconds(300));

    String errorMsg = "error";

    StepVerifier.create(
            rSocket
                .requestResponse(DefaultPayload.create("test"))
                .doOnRequest(
                    ignored ->
                        conn.addToReceivedBuffer(
                            ErrorFrameFlyweight.encode(
                                ByteBufAllocator.DEFAULT,
                                0,
                                new RejectedSetupException(errorMsg)))))
        .expectErrorMatches(
            err -> err instanceof RejectedSetupException && errorMsg.equals(err.getMessage()))
        .verify(Duration.ofSeconds(5));

    assertThat(errors).hasSize(1);
    assertThat(rSocket.isDisposed()).isTrue();
  }

  @Test
  void clientNewStreamsTerminatedAfterRejectSetup() {
    TestDuplexConnection conn = new TestDuplexConnection(Schedulers.single());
    RSocketRequester rSocket =
        new RSocketRequester(
            ByteBufAllocator.DEFAULT,
            conn,
            PayloadDecoder.DEFAULT,
            err -> {},
            StreamErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
            RSocketErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
            StreamIdSupplier.clientSupplier(),
            100_000,
            100_000,
            new KeepAliveHandler.DefaultKeepAliveHandler(conn),
            Duration.ofSeconds(300));

    conn.addToReceivedBuffer(
        ErrorFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, 0, new RejectedSetupException("error")));

    StepVerifier.create(
            rSocket
                .requestResponse(DefaultPayload.create("test"))
                .delaySubscription(Duration.ofMillis(100)))
        .expectErrorMatches(
            err -> err instanceof RejectedSetupException && "error".equals(err.getMessage()))
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void disposeMessageRequesterRSocket() {
    TestDuplexConnection conn = new TestDuplexConnection(Schedulers.single());
    RSocketRequester rSocket =
        new RSocketRequester(
            ByteBufAllocator.DEFAULT,
            conn,
            PayloadDecoder.DEFAULT,
            err -> {},
            StreamErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
            RSocketErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
            StreamIdSupplier.clientSupplier(),
            100_000,
            100_000,
            new KeepAliveHandler.DefaultKeepAliveHandler(conn),
            Duration.ofSeconds(300));

    String errorMsg = "error";

    StepVerifier.create(
            rSocket
                .requestResponse(DefaultPayload.create("test"))
                .doOnRequest(ignored -> rSocket.dispose(errorMsg, false)))
        .expectErrorMatches(
            err -> err instanceof ConnectionErrorException && errorMsg.equals(err.getMessage()))
        .verify(Duration.ofSeconds(5));

    Collection<ByteBuf> sent = conn.getSent();

    assertThat(sent).isNotEmpty();

    ByteBuf lastFrame = Unpooled.EMPTY_BUFFER;
    for (ByteBuf frame : sent) {
      lastFrame = frame;
    }
    assertThat(FrameHeaderFlyweight.frameType(lastFrame)).isEqualTo(FrameType.ERROR);
    assertThat(FrameHeaderFlyweight.streamId(lastFrame)).isEqualTo(0);
    assertThat(ErrorFrameFlyweight.errorCode(lastFrame)).isEqualTo(ErrorCodes.CONNECTION_ERROR);
    assertThat(ErrorFrameFlyweight.dataUtf8(lastFrame)).isEqualTo(errorMsg);

    assertThat(rSocket.isDisposed()).isTrue();
  }

  @Test
  void disposeMessageResponderRSocket() {
    TestDuplexConnection conn = new TestDuplexConnection(Schedulers.single());
    RSocketRequester rSocket =
        new RSocketRequester(
            ByteBufAllocator.DEFAULT,
            conn,
            PayloadDecoder.DEFAULT,
            err -> {},
            StreamErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
            RSocketErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
            StreamIdSupplier.clientSupplier(),
            100_000,
            100_000,
            new KeepAliveHandler.DefaultKeepAliveHandler(conn),
            Duration.ofSeconds(300));

    String errorMsg = "error";

    conn.addToReceivedBuffer(
        ErrorFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, 0, ErrorCodes.CONNECTION_ERROR, errorMsg));

    StepVerifier.create(rSocket.requestResponse(DefaultPayload.create("test")))
        .expectErrorMatches(
            err -> err instanceof ConnectionErrorException && errorMsg.equals(err.getMessage()))
        .verify(Duration.ofSeconds(5));

    assertThat(rSocket.isDisposed()).isTrue();
  }

  @Test
  void poolableConnectionSetup() {
    Payload payload = ByteBufPayload.create("data", "metadata");
    TestClientTransport transport = new TestClientTransport(Schedulers.single());
    Mono<RSocket> rSocket =
        RSocketFactory.connect().setupPayload(payload).transport(transport).start();

    TestDuplexConnection connection = transport.testConnection();
    for (int i = 0; i < 100; i++) {
      rSocket.block();
      Collection<ByteBuf> sent = connection.getSent();
      Assertions.assertThat(sent).hasSize(1);
      ByteBuf setup = sent.iterator().next();
      setup.release();
      sent.clear();
      Assertions.assertThat(setup.refCnt()).isEqualTo(0);
      Assertions.assertThat(payload.refCnt()).isEqualTo(1);
      Assertions.assertThat(payload.getDataUtf8()).isEqualTo("data");
      Assertions.assertThat(payload.getMetadataUtf8()).isEqualTo("metadata");
    }
  }

  private static class RejectingAcceptor implements ServerSocketAcceptor {
    private final String errorMessage;
    private final UnicastProcessor<RSocket> senderRSockets = UnicastProcessor.create();

    public RejectingAcceptor(String errorMessage) {
      this.errorMessage = errorMessage;
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
      senderRSockets.onNext(sendingSocket);
      return Mono.error(new RuntimeException(errorMessage));
    }

    public Mono<RSocket> senderRSocket() {
      return senderRSockets.next();
    }
  }

  private static class SingleConnectionTransport implements ServerTransport<TestCloseable> {

    private final TestDuplexConnection conn = new TestDuplexConnection(Schedulers.single());

    @Override
    public Mono<TestCloseable> start(ConnectionAcceptor acceptor, int frameSizeLimit) {
      return Mono.just(new TestCloseable(acceptor, conn));
    }

    public ByteBuf awaitSent() {
      try {
        return conn.awaitSend();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public void connect() {
      Payload payload = DefaultPayload.create(DefaultPayload.EMPTY_BUFFER);
      ByteBuf setup =
          SetupFrameFlyweight.encode(
              ByteBufAllocator.DEFAULT, false, 0, 42, "mdMime", "dMime", payload);

      conn.addToReceivedBuffer(setup);
    }
  }

  private static class TestCloseable implements Closeable {

    private final DuplexConnection conn;

    TestCloseable(ConnectionAcceptor acceptor, DuplexConnection conn) {
      this.conn = conn;
      Mono.from(acceptor.apply(conn)).subscribe(notUsed -> {}, err -> conn.dispose());
    }

    @Override
    public Mono<Void> onClose() {
      return conn.onClose();
    }

    @Override
    public void dispose() {
      conn.dispose();
    }
  }
}
