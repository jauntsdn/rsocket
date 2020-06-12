/*
 * Copyright 2020 - present Maksym Ostroverkhov.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jauntsdn.rsocket;

import com.jauntsdn.rsocket.exceptions.ConnectionErrorException;
import com.jauntsdn.rsocket.frame.ErrorCodes;
import com.jauntsdn.rsocket.frame.ErrorFrameFlyweight;
import com.jauntsdn.rsocket.frame.FrameHeaderFlyweight;
import com.jauntsdn.rsocket.frame.FrameType;
import com.jauntsdn.rsocket.frame.decoder.PayloadDecoder;
import com.jauntsdn.rsocket.test.util.TestDuplexConnection;
import com.jauntsdn.rsocket.util.DefaultPayload;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class RSocketResponderMetadataPushLimitTest {

  @Test
  void metadataPushOverLimitClosesConnection() {
    String expectedErrorMessage = "[test] metadata-push limit exceeded: 1 over 1000 millis";
    String errorMessagePrefix = "[test] ";
    List<Throwable> errors = new ArrayList<>();
    TestDuplexConnection connection = new TestDuplexConnection();
    RSocketResponder responder =
        rSocketResponder(connection, errors, errorMessagePrefix, 1, Duration.ofSeconds(1));

    responder.metadataPush(DefaultPayload.create("", "metadata-push"));
    Assertions.assertThat(errors).isEmpty();
    Assertions.assertThat(connection.isDisposed()).isFalse();

    responder.metadataPush(DefaultPayload.create("", "metadata-push"));
    Assertions.assertThat(errors).hasSize(1);
    Throwable error = errors.iterator().next();
    Assertions.assertThat(error).isInstanceOf(ConnectionErrorException.class);
    Assertions.assertThat(error.getMessage()).isEqualTo(expectedErrorMessage);
    Assertions.assertThat(connection.isDisposed()).isTrue();

    Collection<ByteBuf> sent = connection.getSent();
    try {
      Assertions.assertThat(sent).hasSize(1);
      ByteBuf frame = sent.iterator().next();
      Assertions.assertThat(FrameHeaderFlyweight.streamId(frame)).isEqualTo(0);
      Assertions.assertThat(FrameHeaderFlyweight.frameType(frame)).isEqualTo(FrameType.ERROR);
      Assertions.assertThat(ErrorFrameFlyweight.errorCode(frame))
          .isEqualTo(ErrorCodes.CONNECTION_ERROR);
      Assertions.assertThat(ErrorFrameFlyweight.dataUtf8(frame)).isEqualTo(expectedErrorMessage);
    } finally {
      sent.forEach(ReferenceCountUtil::safeRelease);
    }
  }

  @Test
  void requestsLimitResetsOverTime() throws InterruptedException {
    List<Throwable> errors = new ArrayList<>();
    TestDuplexConnection connection = new TestDuplexConnection();
    RSocketResponder responder = rSocketResponder(connection, errors, "", 1, Duration.ofSeconds(1));

    responder.metadataPush(DefaultPayload.create("", "metadata-push"));
    Assertions.assertThat(errors).isEmpty();
    Assertions.assertThat(connection.isDisposed()).isFalse();

    Thread.sleep(1100);

    responder.metadataPush(DefaultPayload.create("", "metadata-push"));
    Assertions.assertThat(errors).isEmpty();
    Assertions.assertThat(connection.isDisposed()).isFalse();
  }

  private RSocketResponder rSocketResponder(
      TestDuplexConnection connection,
      List<Throwable> errors,
      String errorPrefix,
      int metadataPushesLimit,
      Duration metadataPushesInterval) {
    return new RSocketResponder(
        ByteBufAllocator.DEFAULT,
        connection,
        new AbstractRSocket() {
          @Override
          public Mono<Void> metadataPush(Payload payload) {
            payload.release();
            return Mono.empty();
          }
        },
        PayloadDecoder.DEFAULT,
        errors::add,
        StreamErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT),
        RSocketErrorMappers.create()
            .sendMapper((errorCode, errorMessage) -> errorPrefix + errorMessage)
            .createErrorMapper(ByteBufAllocator.DEFAULT),
        metadataPushesLimit,
        metadataPushesInterval);
  }
}
