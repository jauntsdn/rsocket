package com.jauntsdn.rsocket;

import com.jauntsdn.rsocket.exceptions.Exceptions;
import com.jauntsdn.rsocket.frame.ErrorFrameFlyweight;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public final class RSocketErrorMappers {
  private RSocketSendMapper rSocketSendMapper;
  private RSocketReceiveMapper rSocketReceiveMapper;

  public static RSocketErrorMappers create() {
    return new RSocketErrorMappers();
  }

  private RSocketErrorMappers() {}

  public RSocketErrorMappers sendMapper(RSocketSendMapper sendMapper) {
    this.rSocketSendMapper = sendMapper;
    return this;
  }

  public RSocketErrorMappers receiveMapper(RSocketReceiveMapper receiveMapper) {
    this.rSocketReceiveMapper = receiveMapper;
    return this;
  }

  RSocketErrorMapper createErrorMapper(ByteBufAllocator allocator) {
    return new RSocketErrorMapper(allocator, rSocketSendMapper, rSocketReceiveMapper);
  }

  interface RSocketSendMapper {

    String map(int errorCode, String errorMessage);
  }

  interface RSocketReceiveMapper {

    Exception map(int errorCode, String errorMessage);
  }

  static class RSocketErrorMapper {
    private final ByteBufAllocator allocator;
    private final RSocketSendMapper rSocketSendMapper;
    private final RSocketReceiveMapper rSocketReceiveMapper;

    private RSocketErrorMapper(
        ByteBufAllocator allocator,
        RSocketSendMapper rSocketSendMapper,
        RSocketReceiveMapper rSocketReceiveMapper) {
      this.allocator = allocator;
      this.rSocketSendMapper = rSocketSendMapper;
      this.rSocketReceiveMapper = rSocketReceiveMapper;
    }

    public ByteBuf sendErrorFrame(int errorCode, String errorMessage) {
      return ErrorFrameFlyweight.encode(
          allocator, 0, errorCode, sendError(errorCode, errorMessage));
    }

    public String sendError(int errorCode, String errorMessage) {
      RSocketSendMapper mapper = this.rSocketSendMapper;
      if (mapper != null) {
        String msg = mapper.map(errorCode, errorMessage);
        if (msg != null) {
          return msg;
        }
      }
      return errorMessage;
    }

    public Exception receiveErrorFrame(ByteBuf errorFrame) {
      int errorCode = ErrorFrameFlyweight.errorCode(errorFrame);
      String errorMessage = ErrorFrameFlyweight.dataUtf8(errorFrame);
      return receiveError(errorCode, errorMessage);
    }

    public Exception receiveError(int errorCode, String errorMessage) {
      RSocketReceiveMapper mapper = this.rSocketReceiveMapper;
      if (mapper != null) {
        Exception e = mapper.map(errorCode, errorMessage);
        if (e != null) {
          return e;
        }
      }
      return Exceptions.from(errorCode, errorMessage);
    }
  }
}
