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
import com.jauntsdn.rsocket.exceptions.InvalidSetupException;
import com.jauntsdn.rsocket.frame.*;
import com.jauntsdn.rsocket.internal.ValidatingConnection;
import com.jauntsdn.rsocket.test.util.TestDuplexConnection;
import com.jauntsdn.rsocket.util.EmptyPayload;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.test.StepVerifier;

public class ValidatingConnectionTest {

  private TestDuplexConnection testConnection;

  @BeforeEach
  void setUp() {
    testConnection = new TestDuplexConnection();
  }

  @AfterEach
  void tearDown() {
    testConnection.getSent().forEach(ReferenceCountUtil::safeRelease);
  }

  @ParameterizedTest
  @MethodSource("invalidInitialFrames")
  void serverInvalidSetup(ByteBuf frame) {
    ValidatingConnection validatingConnection = ValidatingConnection.ofServer(testConnection);

    StepVerifier stepVerifier =
        validatingConnection
            .receive()
            .as(StepVerifier::create)
            .expectErrorMatches(
                err ->
                    err instanceof InvalidSetupException
                        && err.getMessage() != null
                        && err.getMessage().startsWith("Unexpected start frame"))
            .verifyLater();

    testConnection.addToReceivedBuffer(frame);
    try {
      stepVerifier.verify(Duration.ofSeconds(5));
      Assertions.assertThat(frame.refCnt()).isEqualTo(0);
    } finally {
      tryRelease(frame);
    }
  }

  @Test
  void serverInvalidFrameType() {
    ValidatingConnection validatingConnection = ValidatingConnection.ofServer(testConnection);

    ByteBuf setup =
        SetupFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, false, 10_000, 1000_000, "", "", EmptyPayload.INSTANCE);
    short encodedType = (0x0F << 10);
    ByteBuf invalidFrame =
        UnpooledByteBufAllocator.DEFAULT.buffer().writeInt(1).writeShort(encodedType);

    StepVerifier stepVerifier =
        validatingConnection
            .receive()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .expectErrorMatches(
                err ->
                    err instanceof IllegalArgumentException
                        && "Unknown incoming frame type: 15".equals(err.getMessage()))
            .verifyLater();

    testConnection.addToReceivedBuffer(setup);
    testConnection.addToReceivedBuffer(invalidFrame);

    try {
      stepVerifier.verify(Duration.ofSeconds(5));
      Assertions.assertThat(setup.refCnt()).isEqualTo(1);
      Assertions.assertThat(invalidFrame.refCnt()).isEqualTo(0);
    } finally {
      tryRelease(setup);
      tryRelease(invalidFrame);
    }
  }

  @ParameterizedTest
  @MethodSource("validInitialFrames")
  void serverSetup(ByteBuf initialFrame) {
    ValidatingConnection validatingConnection = ValidatingConnection.ofServer(testConnection);

    StepVerifier stepVerifier =
        validatingConnection
            .receive()
            .take(1)
            .then()
            .as(StepVerifier::create)
            .expectComplete()
            .verifyLater();

    testConnection.addToReceivedBuffer(initialFrame);

    try {
      stepVerifier.verify(Duration.ofSeconds(5));
      Assertions.assertThat(initialFrame.refCnt()).isEqualTo(1);
    } finally {
      tryRelease(initialFrame);
    }
  }

  @Test
  void serverSetupSetupDuplicate() {
    ValidatingConnection validatingConnection = ValidatingConnection.ofServer(testConnection);

    ByteBuf setup =
        SetupFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT,
            true,
            1000,
            30_000,
            "application/octet-stream",
            "application/octet-stream",
            EmptyPayload.INSTANCE);

    ByteBuf setupCopy = setup.copy();

    StepVerifier stepVerifier =
        validatingConnection
            .receive()
            .take(Duration.ofMillis(100))
            .as(StepVerifier::create)
            .expectNextCount(1)
            .expectComplete()
            .verifyLater();

    testConnection.addToReceivedBuffer(setup, setupCopy);

    try {
      stepVerifier.verify(Duration.ofSeconds(5));
      Assertions.assertThat(setup.refCnt()).isEqualTo(1);
      Assertions.assertThat(setupCopy.refCnt()).isEqualTo(0);
    } finally {
      tryRelease(setup);
      tryRelease(setupCopy);
    }
  }

  @Test
  void serverSetupResumeDuplicate() {
    ValidatingConnection validatingConnection = ValidatingConnection.ofServer(testConnection);

    ByteBuf setup =
        SetupFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT,
            true,
            1000,
            30_000,
            "application/octet-stream",
            "application/octet-stream",
            EmptyPayload.INSTANCE);

    ByteBuf token = Unpooled.wrappedBuffer("token".getBytes(StandardCharsets.UTF_8));
    ByteBuf resume = ResumeFrameFlyweight.encode(UnpooledByteBufAllocator.DEFAULT, token, 21, 12);

    StepVerifier stepVerifier =
        validatingConnection
            .receive()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .expectErrorMatches(
                err ->
                    err instanceof ConnectionErrorException
                        && err.getMessage() != null
                        && err.getMessage().startsWith("Unexpected duplicated setup frame"))
            .verifyLater();

    testConnection.addToReceivedBuffer(setup, resume);

    try {
      stepVerifier.verify(Duration.ofSeconds(5));
      Assertions.assertThat(setup.refCnt()).isEqualTo(1);
      Assertions.assertThat(resume.refCnt()).isEqualTo(0);
      Collection<ByteBuf> sent = testConnection.getSent();
      Assertions.assertThat(sent.size()).isEqualTo(1);
      ByteBuf frame = sent.iterator().next();
      FrameType frameType = FrameHeaderFlyweight.frameType(frame);
      int streamId = FrameHeaderFlyweight.streamId(frame);
      Assertions.assertThat(frameType).isEqualTo(FrameType.ERROR);
      Assertions.assertThat(streamId).isEqualTo(0);
      String message = ErrorFrameFlyweight.dataUtf8(frame);
      Assertions.assertThat(message).startsWith("Unexpected duplicated setup frame");

    } finally {
      tryRelease(setup);
      tryRelease(resume);
    }
  }

  @ParameterizedTest
  @MethodSource("resumeDuplicateFrames")
  void serverResumeDuplicate(Collection<ByteBuf> frames) {
    ValidatingConnection validatingConnection = ValidatingConnection.ofServer(testConnection);

    StepVerifier stepVerifier =
        validatingConnection
            .receive()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .expectErrorMatches(
                err ->
                    err instanceof ConnectionErrorException
                        && err.getMessage() != null
                        && err.getMessage().startsWith("Unexpected duplicated setup frame"))
            .verifyLater();
    Iterator<ByteBuf> it = frames.iterator();
    ByteBuf first = it.next();
    ByteBuf second = it.next();
    testConnection.addToReceivedBuffer(first, second);

    try {
      stepVerifier.verify(Duration.ofSeconds(5));
      Assertions.assertThat(first.refCnt()).isEqualTo(1);
      Assertions.assertThat(second.refCnt()).isEqualTo(0);
      Collection<ByteBuf> sent = testConnection.getSent();
      Assertions.assertThat(sent.size()).isEqualTo(1);
      ByteBuf frame = sent.iterator().next();
      FrameType frameType = FrameHeaderFlyweight.frameType(frame);
      int streamId = FrameHeaderFlyweight.streamId(frame);
      Assertions.assertThat(frameType).isEqualTo(FrameType.ERROR);
      Assertions.assertThat(streamId).isEqualTo(0);
      String message = ErrorFrameFlyweight.dataUtf8(frame);
      Assertions.assertThat(message).startsWith("Unexpected duplicated setup frame");

    } finally {
      tryRelease(first);
      tryRelease(second);
    }
  }

  @Test
  void serverNonZeroMetadataPush() {
    ValidatingConnection validatingConnection = ValidatingConnection.ofServer(testConnection);

    ByteBuf setup =
        SetupFrameFlyweight.encode(
            UnpooledByteBufAllocator.DEFAULT,
            false,
            10_000,
            1000_000,
            "",
            "",
            EmptyPayload.INSTANCE);

    ByteBuf metadata =
        MetadataPushFrameFlyweight.encode(
            UnpooledByteBufAllocator.DEFAULT,
            Unpooled.wrappedBuffer("metadata".getBytes(StandardCharsets.UTF_8)));

    int nonZeroStreamId = 3;

    metadata.markWriterIndex();
    metadata.writerIndex(0);
    metadata.writeInt(nonZeroStreamId);
    metadata.resetWriterIndex();

    StepVerifier stepVerifier =
        validatingConnection
            .receive()
            .take(Duration.ofMillis(100))
            .as(StepVerifier::create)
            .expectNextCount(1)
            .expectComplete()
            .verifyLater();

    testConnection.addToReceivedBuffer(setup);
    testConnection.addToReceivedBuffer(metadata);

    try {
      stepVerifier.verify(Duration.ofSeconds(5));
      Assertions.assertThat(metadata.refCnt()).isEqualTo(0);
    } finally {
      tryRelease(metadata);
    }
  }

  @ParameterizedTest
  @MethodSource("setupErrors")
  void serverSetupErrors(ByteBuf setupError) {
    ValidatingConnection validatingConnection = ValidatingConnection.ofServer(testConnection);

    ByteBuf setup =
        SetupFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT,
            true,
            1000,
            30_000,
            "application/octet-stream",
            "application/octet-stream",
            EmptyPayload.INSTANCE);

    StepVerifier stepVerifier =
        validatingConnection
            .receive()
            .take(Duration.ofMillis(100))
            .as(StepVerifier::create)
            .expectNextCount(1)
            .expectComplete()
            .verifyLater();

    testConnection.addToReceivedBuffer(setup);
    testConnection.addToReceivedBuffer(setupError);

    try {
      stepVerifier.verify(Duration.ofSeconds(5));
      Assertions.assertThat(setupError.refCnt()).isEqualTo(0);
    } finally {
      tryRelease(setup);
      tryRelease(setupError);
    }
  }

  static Stream<ByteBuf> validInitialFrames() {
    ByteBuf setup =
        SetupFrameFlyweight.encode(
            UnpooledByteBufAllocator.DEFAULT,
            false,
            10_000,
            1000_000,
            "",
            "",
            EmptyPayload.INSTANCE);
    ByteBuf resume =
        ResumeFrameFlyweight.encode(
            UnpooledByteBufAllocator.DEFAULT,
            Unpooled.wrappedBuffer("token".getBytes(StandardCharsets.UTF_8)),
            21,
            12);

    return Stream.of(setup, resume);
  }

  static Stream<ByteBuf> invalidInitialFrames() {
    int invalidStreamId = 3;

    ByteBuf invalidSetup =
        SetupFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT,
            true,
            1000,
            30_000,
            "application/octet-stream",
            "application/octet-stream",
            EmptyPayload.INSTANCE);
    setStreamId(invalidSetup, invalidStreamId);

    ByteBuf token = Unpooled.wrappedBuffer("token".getBytes(StandardCharsets.UTF_8));
    ByteBuf invalidResume =
        ResumeFrameFlyweight.encode(UnpooledByteBufAllocator.DEFAULT, token, 21, 12);
    setStreamId(invalidResume, invalidStreamId);

    ByteBuf request =
        RequestResponseFrameFlyweight.encode(
            UnpooledByteBufAllocator.DEFAULT, 1, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);

    return Stream.of(invalidSetup, invalidResume, request);
  }

  static Stream<Collection<ByteBuf>> resumeDuplicateFrames() {
    ByteBuf setup =
        SetupFrameFlyweight.encode(
            UnpooledByteBufAllocator.DEFAULT,
            true,
            1000,
            30_000,
            "application/octet-stream",
            "application/octet-stream",
            EmptyPayload.INSTANCE);

    ByteBuf token = Unpooled.wrappedBuffer("token".getBytes(StandardCharsets.UTF_8));
    ByteBuf resume = ResumeFrameFlyweight.encode(UnpooledByteBufAllocator.DEFAULT, token, 21, 12);

    return Stream.of(Arrays.asList(resume, setup), Arrays.asList(resume.copy(), resume.copy()));
  }

  static Stream<ByteBuf> setupErrors() {
    ByteBuf invalidSetup =
        ErrorFrameFlyweight.encode(
            UnpooledByteBufAllocator.DEFAULT, 0, ErrorCodes.INVALID_SETUP, "");
    ByteBuf unsupportedSetup =
        ErrorFrameFlyweight.encode(
            UnpooledByteBufAllocator.DEFAULT, 0, ErrorCodes.UNSUPPORTED_SETUP, "");
    ByteBuf rejectedSetup =
        ErrorFrameFlyweight.encode(
            UnpooledByteBufAllocator.DEFAULT, 0, ErrorCodes.REJECTED_SETUP, "");
    ByteBuf resjectedResume =
        ErrorFrameFlyweight.encode(
            UnpooledByteBufAllocator.DEFAULT, 0, ErrorCodes.REJECTED_RESUME, "");

    return Stream.of(invalidSetup, unsupportedSetup, rejectedSetup, resjectedResume);
  }

  private static void setStreamId(ByteBuf frame, int streamId) {
    frame.markWriterIndex();
    frame.writerIndex(0);
    frame.writeInt(streamId);
    frame.resetWriterIndex();
  }

  private static void tryRelease(ByteBuf resume) {
    if (resume.refCnt() > 0) {
      resume.release();
    }
  }
}
