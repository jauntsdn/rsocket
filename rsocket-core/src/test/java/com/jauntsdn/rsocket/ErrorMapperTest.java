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

import static com.jauntsdn.rsocket.StreamErrorMappers.*;

import com.jauntsdn.rsocket.exceptions.ApplicationErrorException;
import com.jauntsdn.rsocket.exceptions.Exceptions;
import com.jauntsdn.rsocket.frame.ErrorCodes;
import com.jauntsdn.rsocket.frame.ErrorFrameFlyweight;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class ErrorMapperTest {

  @Test
  void noSendMapper() {
    StreamErrorMappers mappers = StreamErrorMappers.create();
    ErrorFrameMapper mapper = mappers.createErrorFrameMapper(ByteBufAllocator.DEFAULT);
    ByteBuf errorFrame =
        mapper.streamErrorToFrame(1, StreamType.RESPONSE, new RuntimeException("error"));
    Assertions.assertThat(errorFrame).isNotNull();
    Assertions.assertThat(ErrorFrameFlyweight.errorCode(errorFrame))
        .isEqualTo(ErrorCodes.APPLICATION_ERROR);
    Assertions.assertThat(ErrorFrameFlyweight.dataUtf8(errorFrame)).isEqualTo("error");
  }

  @Test
  void sendApplicationError() {
    StreamErrorMappers mappers =
        StreamErrorMappers.create()
            .sendMapper(
                new TestSendMapper(
                    IllegalStateException.class,
                    StreamSendMapper.Error.application("application_error")));
    ErrorFrameMapper mapper = mappers.createErrorFrameMapper(ByteBufAllocator.DEFAULT);
    ByteBuf errorFrame =
        mapper.streamErrorToFrame(1, StreamType.RESPONSE, new IllegalStateException("error"));
    Assertions.assertThat(errorFrame).isNotNull();
    Assertions.assertThat(ErrorFrameFlyweight.errorCode(errorFrame))
        .isEqualTo(ErrorCodes.APPLICATION_ERROR);
    Assertions.assertThat(ErrorFrameFlyweight.dataUtf8(errorFrame)).isEqualTo("application_error");
  }

  @Test
  void sendCancelError() {
    StreamErrorMappers mappers =
        StreamErrorMappers.create()
            .sendMapper(
                new TestSendMapper(
                    IllegalStateException.class, StreamSendMapper.Error.cancel("cancel_error")));
    ErrorFrameMapper mapper = mappers.createErrorFrameMapper(ByteBufAllocator.DEFAULT);
    ByteBuf errorFrame =
        mapper.streamErrorToFrame(1, StreamType.RESPONSE, new IllegalStateException("error"));
    Assertions.assertThat(errorFrame).isNotNull();
    Assertions.assertThat(ErrorFrameFlyweight.errorCode(errorFrame)).isEqualTo(ErrorCodes.CANCELED);
    Assertions.assertThat(ErrorFrameFlyweight.dataUtf8(errorFrame)).isEqualTo("cancel_error");
  }

  @Test
  void sendRejectError() {
    StreamErrorMappers mappers =
        StreamErrorMappers.create()
            .sendMapper(
                new TestSendMapper(
                    IllegalStateException.class, StreamSendMapper.Error.reject("reject_error")));
    ErrorFrameMapper mapper = mappers.createErrorFrameMapper(ByteBufAllocator.DEFAULT);
    ByteBuf errorFrame =
        mapper.streamErrorToFrame(1, StreamType.RESPONSE, new IllegalStateException("error"));
    Assertions.assertThat(errorFrame).isNotNull();
    Assertions.assertThat(ErrorFrameFlyweight.errorCode(errorFrame)).isEqualTo(ErrorCodes.REJECTED);
    Assertions.assertThat(ErrorFrameFlyweight.dataUtf8(errorFrame)).isEqualTo("reject_error");
  }

  @Test
  void sendInvalidError() {
    StreamErrorMappers mappers =
        StreamErrorMappers.create()
            .sendMapper(
                new TestSendMapper(
                    IllegalStateException.class, StreamSendMapper.Error.invalid("invalid_error")));
    ErrorFrameMapper mapper = mappers.createErrorFrameMapper(ByteBufAllocator.DEFAULT);
    ByteBuf errorFrame =
        mapper.streamErrorToFrame(1, StreamType.RESPONSE, new IllegalStateException("error"));
    Assertions.assertThat(errorFrame).isNotNull();
    Assertions.assertThat(ErrorFrameFlyweight.errorCode(errorFrame)).isEqualTo(ErrorCodes.INVALID);
    Assertions.assertThat(ErrorFrameFlyweight.dataUtf8(errorFrame)).isEqualTo("invalid_error");
  }

  @Test
  void sendMapperReturnsNull() {
    StreamErrorMappers mappers = StreamErrorMappers.create().sendMapper((streamType, t) -> null);
    ErrorFrameMapper mapper = mappers.createErrorFrameMapper(ByteBufAllocator.DEFAULT);
    ByteBuf errorFrame =
        mapper.streamErrorToFrame(1, StreamType.RESPONSE, new IllegalStateException("error"));
    Assertions.assertThat(errorFrame).isNotNull();
    Assertions.assertThat(ErrorFrameFlyweight.errorCode(errorFrame))
        .isEqualTo(ErrorCodes.APPLICATION_ERROR);
    Assertions.assertThat(ErrorFrameFlyweight.dataUtf8(errorFrame)).isEqualTo("error");
  }

  @Test
  void noReceiveMapper() {
    StreamErrorMappers mappers = StreamErrorMappers.create();
    ErrorFrameMapper mapper = mappers.createErrorFrameMapper(ByteBufAllocator.DEFAULT);
    ByteBuf errorFrame =
        ErrorFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, 1, ErrorCodes.APPLICATION_ERROR, "error");
    Throwable error = mapper.streamFrameToError(errorFrame, StreamType.RESPONSE);
    Assertions.assertThat(error).isInstanceOf(ApplicationErrorException.class);
    Assertions.assertThat(error.getMessage()).isEqualTo("error");
  }

  @Test
  void receiveLeaseExhausted() {
    StreamErrorMappers mappers = StreamErrorMappers.create();
    ErrorFrameMapper mapper = mappers.createErrorFrameMapper(ByteBufAllocator.DEFAULT);
    ByteBuf errorFrame =
        ErrorFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, 1, ErrorCodes.REJECTED, Exceptions.LEASE_EXHAUSTED_MESSAGE);

    Throwable error = mapper.streamFrameToError(errorFrame, StreamType.RESPONSE);
    Assertions.assertThat(error).isSameAs(Exceptions.LEASE_EXHAUSTED_EXCEPTION);
  }

  @Test
  void receiveLeaseExpired() {
    StreamErrorMappers mappers = StreamErrorMappers.create();
    ErrorFrameMapper mapper = mappers.createErrorFrameMapper(ByteBufAllocator.DEFAULT);
    ByteBuf errorFrame =
        ErrorFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, 1, ErrorCodes.REJECTED, Exceptions.LEASE_EXPIRED_MESSAGE);

    Throwable error = mapper.streamFrameToError(errorFrame, StreamType.RESPONSE);
    Assertions.assertThat(error).isSameAs(Exceptions.LEASE_EXPIRED_EXCEPTION);
  }

  @Test
  void receiveApplicationError() {
    Exception testException = new TestException("application_error");
    StreamErrorMappers mappers =
        StreamErrorMappers.create()
            .receiveMapper(
                new TestReceiveMapper(StreamReceiveMapper.ErrorType.APPLICATION, testException));
    ErrorFrameMapper mapper = mappers.createErrorFrameMapper(ByteBufAllocator.DEFAULT);
    ByteBuf errorFrame =
        ErrorFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, 1, ErrorCodes.APPLICATION_ERROR, "error");
    Throwable error = mapper.streamFrameToError(errorFrame, StreamType.RESPONSE);
    Assertions.assertThat(error).isInstanceOf(TestException.class);
    Assertions.assertThat(error.getMessage()).isEqualTo("application_error");
  }

  @Test
  void receiveCancelError() {
    Exception testException = new TestException("cancel_error");
    StreamErrorMappers mappers =
        StreamErrorMappers.create()
            .receiveMapper(
                new TestReceiveMapper(StreamReceiveMapper.ErrorType.CANCELED, testException));
    ErrorFrameMapper mapper = mappers.createErrorFrameMapper(ByteBufAllocator.DEFAULT);
    ByteBuf errorFrame =
        ErrorFrameFlyweight.encode(ByteBufAllocator.DEFAULT, 1, ErrorCodes.CANCELED, "error");
    Throwable error = mapper.streamFrameToError(errorFrame, StreamType.RESPONSE);
    Assertions.assertThat(error).isInstanceOf(TestException.class);
    Assertions.assertThat(error.getMessage()).isEqualTo("cancel_error");
  }

  @Test
  void receiveInvalidError() {
    Exception testException = new TestException("cancel_error");
    StreamErrorMappers mappers =
        StreamErrorMappers.create()
            .receiveMapper(
                new TestReceiveMapper(StreamReceiveMapper.ErrorType.INVALID, testException));
    ErrorFrameMapper mapper = mappers.createErrorFrameMapper(ByteBufAllocator.DEFAULT);
    ByteBuf errorFrame =
        ErrorFrameFlyweight.encode(ByteBufAllocator.DEFAULT, 1, ErrorCodes.INVALID, "error");
    Throwable error = mapper.streamFrameToError(errorFrame, StreamType.RESPONSE);
    Assertions.assertThat(error).isInstanceOf(TestException.class);
    Assertions.assertThat(error.getMessage()).isEqualTo("cancel_error");
  }

  @Test
  void receiveMapperReturnsNull() {
    StreamErrorMappers mappers =
        StreamErrorMappers.create().receiveMapper((streamType, errorType, errorMessage) -> null);
    ErrorFrameMapper mapper = mappers.createErrorFrameMapper(ByteBufAllocator.DEFAULT);
    ByteBuf errorFrame =
        ErrorFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, 1, ErrorCodes.APPLICATION_ERROR, "error");
    Throwable error = mapper.streamFrameToError(errorFrame, StreamType.RESPONSE);
    Assertions.assertThat(error).isInstanceOf(ApplicationErrorException.class);
    Assertions.assertThat(error.getMessage()).isEqualTo("error");
  }

  private static class TestSendMapper implements StreamSendMapper {
    private final Class<?> errorType;
    private final Error error;

    public TestSendMapper(Class<?> errorType, Error error) {
      this.errorType = errorType;
      this.error = error;
    }

    @Override
    public Error map(StreamType streamType, Throwable t) {
      if (errorType.isAssignableFrom(t.getClass())) {
        return error;
      }
      return null;
    }
  }

  private static class TestReceiveMapper implements StreamReceiveMapper {
    private final ErrorType errorType;
    private final Throwable error;

    public TestReceiveMapper(ErrorType errorType, Throwable error) {
      this.errorType = errorType;
      this.error = error;
    }

    @Override
    public Throwable map(StreamType streamType, ErrorType errorType, String errorMessage) {
      if (this.errorType.equals(errorType)) {
        return error;
      }
      return null;
    }
  }

  private static final class TestException extends RuntimeException {
    public TestException() {}

    public TestException(String message) {
      super(message);
    }
  }
}
