package com.jauntsdn.rsocket;

import com.jauntsdn.rsocket.exceptions.Exceptions;
import com.jauntsdn.rsocket.frame.ErrorCodes;
import com.jauntsdn.rsocket.frame.ErrorFrameFlyweight;
import com.jauntsdn.rsocket.frame.FrameHeaderFlyweight;
import com.jauntsdn.rsocket.frame.FrameType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class RSocketErrorMapperTest {

  @Test
  void sendNoMapper() {
    RSocketErrorMappers.RSocketErrorMapper errorMapper =
        RSocketErrorMappers.create().createErrorMapper(ByteBufAllocator.DEFAULT);
    String errorMessage = "error";
    ByteBuf frame = errorMapper.rSocketErrorToFrame(ErrorCodes.CONNECTION_ERROR, errorMessage);
    Assertions.assertThat(FrameHeaderFlyweight.frameType(frame)).isEqualTo(FrameType.ERROR);
    Assertions.assertThat(ErrorFrameFlyweight.errorCode(frame))
        .isEqualTo(ErrorCodes.CONNECTION_ERROR);
    Assertions.assertThat(ErrorFrameFlyweight.dataUtf8(frame)).isEqualTo(errorMessage);
  }

  @Test
  void sendMapperReturnsNull() {
    RSocketErrorMappers.RSocketErrorMapper errorMapper =
        RSocketErrorMappers.create()
            .sendMapper((errorCode, errorMessage) -> null)
            .createErrorMapper(ByteBufAllocator.DEFAULT);
    String errorMessage = "error";
    ByteBuf frame = errorMapper.rSocketErrorToFrame(ErrorCodes.CONNECTION_ERROR, errorMessage);
    Assertions.assertThat(FrameHeaderFlyweight.frameType(frame)).isEqualTo(FrameType.ERROR);
    Assertions.assertThat(ErrorFrameFlyweight.errorCode(frame))
        .isEqualTo(ErrorCodes.CONNECTION_ERROR);
    Assertions.assertThat(ErrorFrameFlyweight.dataUtf8(frame)).isEqualTo(errorMessage);
  }

  @Test
  void sendMapper() {
    String errorMessage = "error";
    String mappedMessage = "mapped";
    RSocketErrorMappers.RSocketErrorMapper errorMapper =
        RSocketErrorMappers.create()
            .sendMapper((code, message) -> mappedMessage)
            .createErrorMapper(ByteBufAllocator.DEFAULT);
    ByteBuf frame = errorMapper.rSocketErrorToFrame(ErrorCodes.CONNECTION_ERROR, errorMessage);
    Assertions.assertThat(FrameHeaderFlyweight.frameType(frame)).isEqualTo(FrameType.ERROR);
    Assertions.assertThat(ErrorFrameFlyweight.errorCode(frame))
        .isEqualTo(ErrorCodes.CONNECTION_ERROR);
    Assertions.assertThat(ErrorFrameFlyweight.dataUtf8(frame)).isEqualTo(mappedMessage);
  }

  @Test
  void receiveNoMapper() {
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    RSocketErrorMappers.RSocketErrorMapper errorMapper =
        RSocketErrorMappers.create().createErrorMapper(allocator);
    String errorMessage = "error";
    ByteBuf frame =
        ErrorFrameFlyweight.encode(allocator, 0, ErrorCodes.CONNECTION_ERROR, errorMessage);
    Exception e = errorMapper.frameToRSocketError(frame);
    Assertions.assertThat(e).isInstanceOf(Exceptions.from(frame).getClass());
    Assertions.assertThat(e.getMessage()).isEqualTo(e.getMessage());
  }

  @Test
  void receiveMapperReturnsNull() {
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    RSocketErrorMappers.RSocketErrorMapper errorMapper =
        RSocketErrorMappers.create()
            .receiveMapper((errorCode, errorMessage) -> null)
            .createErrorMapper(allocator);
    String errorMessage = "error";
    ByteBuf frame =
        ErrorFrameFlyweight.encode(allocator, 0, ErrorCodes.CONNECTION_ERROR, errorMessage);
    Exception e = errorMapper.frameToRSocketError(frame);
    Assertions.assertThat(e).isInstanceOf(Exceptions.from(frame).getClass());
    Assertions.assertThat(e.getMessage()).isEqualTo(e.getMessage());
  }

  @Test
  void receiveMapper() {
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    String errorMessage = "error";
    String mappedMessage = "mapped";
    RSocketErrorMappers.RSocketErrorMapper errorMapper =
        RSocketErrorMappers.create()
            .receiveMapper((code, message) -> new IllegalArgumentException(mappedMessage))
            .createErrorMapper(allocator);

    ByteBuf frame =
        ErrorFrameFlyweight.encode(allocator, 0, ErrorCodes.CONNECTION_ERROR, errorMessage);
    Exception e = errorMapper.frameToRSocketError(frame);
    Assertions.assertThat(e).isInstanceOf(IllegalArgumentException.class);
    Assertions.assertThat(e.getMessage()).isEqualTo(mappedMessage);
  }
}
