package com.jauntsdn.rsocket;

import com.jauntsdn.rsocket.exceptions.ConnectionErrorException;
import com.jauntsdn.rsocket.frame.*;
import com.jauntsdn.rsocket.frame.decoder.PayloadDecoder;
import com.jauntsdn.rsocket.keepalive.KeepAliveHandler;
import com.jauntsdn.rsocket.test.util.TestDuplexConnection;
import com.jauntsdn.rsocket.util.DefaultPayload;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class RSocketRequesterEdgeCasesTest {

  @ParameterizedTest
  @MethodSource("interactions")
  void streamChannelOverflow(Function<RSocket, Flux<Payload>> interaction) {
    TestDuplexConnection connection = new TestDuplexConnection(Schedulers.single());
    List<Throwable> errors = new ArrayList<>();
    RSocketRequester requester = rSocketRequester(connection, errors);

    interaction.apply(requester).subscribe(new TestSubscriber());

    Collection<ByteBuf> sent = connection.getSent();
    try {
      for (int i = 0; i < 10; i++) {
        connection.addToReceivedBuffer(
            PayloadFrameFlyweight.encode(
                ByteBufAllocator.DEFAULT, 1, false, false, true, DefaultPayload.create("", "")));
      }
      Assertions.assertThat(sent).hasSize(2);
      Iterator<ByteBuf> it = sent.iterator();
      ByteBuf frame = it.next();
      Assertions.assertThat(FrameHeaderFlyweight.frameType(frame))
          .isIn(FrameType.REQUEST_STREAM, FrameType.REQUEST_CHANNEL);
      frame = it.next();
      Assertions.assertThat(FrameHeaderFlyweight.frameType(frame)).isEqualTo(FrameType.ERROR);
      Assertions.assertThat(FrameHeaderFlyweight.streamId(frame)).isEqualTo(0);
      Assertions.assertThat(ErrorFrameFlyweight.errorCode(frame))
          .isEqualTo(ErrorCodes.CONNECTION_ERROR);
      Assertions.assertThat(ErrorFrameFlyweight.dataUtf8(frame))
          .isEqualTo("Requester stream received more frames than demanded with requestN");

      Assertions.assertThat(errors).hasSize(1);
      Throwable error = errors.iterator().next();
      Assertions.assertThat(error).isInstanceOf(ConnectionErrorException.class);
      Assertions.assertThat(error.getMessage())
          .isEqualTo("Requester stream received more frames than demanded with requestN");
      Assertions.assertThat(connection.isDisposed()).isTrue();
    } finally {
      sent.forEach(ReferenceCountUtil::safeRelease);
    }
  }

  private static RSocketRequester rSocketRequester(
      TestDuplexConnection connection, List<Throwable> errors) {
    return new RSocketRequester(
        ByteBufAllocator.DEFAULT,
        connection,
        PayloadDecoder.DEFAULT,
        errors::add,
        StreamErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
        RSocketErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
        StreamIdSupplier.clientSupplier(),
        100_000,
        100_000,
        new KeepAliveHandler.DefaultKeepAliveHandler(connection),
        Duration.ofSeconds(100),
        true);
  }

  static Stream<Function<RSocket, Flux<Payload>>> interactions() {
    return Stream.of(
        rSocket -> rSocket.requestStream(DefaultPayload.create("")),
        rSocket ->
            rSocket.requestChannel(Mono.just(DefaultPayload.create("")).concatWith(Mono.never())));
  }

  private static class TestSubscriber implements Subscriber<Payload> {
    @Override
    public void onSubscribe(Subscription s) {
      s.request(1);
    }

    @Override
    public void onNext(Payload payload) {
      payload.release();
    }

    @Override
    public void onError(Throwable t) {}

    @Override
    public void onComplete() {}
  }
}
