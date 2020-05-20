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

import com.jauntsdn.rsocket.exceptions.Exceptions;
import com.jauntsdn.rsocket.exceptions.RejectedException;
import com.jauntsdn.rsocket.frame.ErrorCodes;
import com.jauntsdn.rsocket.frame.ErrorFrameFlyweight;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import javax.annotation.Nullable;

/** Configures custom error mappers of incoming and outgoing streams. */
public final class StreamErrorMappers {
  private StreamSendMapper streamSendMapper;
  private StreamReceiveMapper streamReceiveMapper;

  private StreamErrorMappers() {}

  public static StreamErrorMappers create() {
    return new StreamErrorMappers();
  }

  /**
   * @param streamSendMapper Optionally sets outgoing stream error mapper
   * @return this {@link StreamErrorMappers} instance
   */
  public StreamErrorMappers sendMapper(StreamSendMapper streamSendMapper) {
    this.streamSendMapper = streamSendMapper;
    return this;
  }

  /**
   * @param streamReceiveMapper Optionally sets incoming strean error mapper
   * @return this {@link StreamErrorMappers} instance
   */
  public StreamErrorMappers receiveMapper(StreamReceiveMapper streamReceiveMapper) {
    this.streamReceiveMapper = streamReceiveMapper;
    return this;
  }

  StreamErrorMapper createErrorMapper(ByteBufAllocator allocator) {
    return new StreamErrorMapper(allocator, streamSendMapper, streamReceiveMapper);
  }

  static final class StreamErrorMapper {
    private final ByteBufAllocator allocator;
    private final StreamSendMapper streamSendMapper;
    private final StreamReceiveMapper streamReceiveMapper;

    private StreamErrorMapper(
        ByteBufAllocator allocator,
        StreamSendMapper streamSendMapper,
        StreamReceiveMapper streamReceiveMapper) {
      this.allocator = allocator;
      this.streamSendMapper = streamSendMapper;
      this.streamReceiveMapper = streamReceiveMapper;
    }

    public ByteBuf streamErrorToFrame(int streamId, StreamType streamType, Throwable t) {
      ByteBufAllocator allocator = this.allocator;

      if (t instanceof RejectedException) {
        String errorMessage = t.getMessage();
        if (errorMessage != null && isLeaseMessage(errorMessage)) {
          return ErrorFrameFlyweight.encode(allocator, streamId, t);
        }
      }

      StreamSendMapper mapper = this.streamSendMapper;
      if (mapper == null) {
        return ErrorFrameFlyweight.encode(allocator, streamId, t);
      }

      StreamSendMapper.Error e = mapper.map(streamType, t);
      if (e == null) {
        return ErrorFrameFlyweight.encode(allocator, streamId, t);
      }
      return ErrorFrameFlyweight.encode(allocator, streamId, e.code(), e.message());
    }

    public Throwable streamFrameToError(ByteBuf frame, StreamType streamType) {
      int errorCode = ErrorFrameFlyweight.errorCode(frame);
      String errorMessage = ErrorFrameFlyweight.dataUtf8(frame);

      if (errorCode == ErrorCodes.REJECTED) {
        switch (errorMessage) {
          case Exceptions.LEASE_EXHAUSTED_MESSAGE:
            return Exceptions.LEASE_EXHAUSTED_EXCEPTION;
          case Exceptions.LEASE_EXPIRED_MESSAGE:
            return Exceptions.LEASE_EXPIRED_EXCEPTION;
          default:
            /*noop*/
        }
      }
      StreamReceiveMapper mapper = this.streamReceiveMapper;

      if (mapper == null) {
        return Exceptions.from(frame);
      }

      StreamReceiveMapper.ErrorType errorType = StreamReceiveMapper.ErrorType.fromCode(errorCode);
      Throwable e = mapper.map(streamType, errorType, errorMessage);
      if (e == null) {
        e = Exceptions.from(errorCode, errorMessage);
      }
      return e;
    }
  }

  private static boolean isLeaseMessage(String message) {
    return Exceptions.LEASE_EXHAUSTED_MESSAGE.equals(message)
        || Exceptions.LEASE_EXPIRED_MESSAGE.equals(message);
  }

  /** Maps stream {@link Throwable} error to RSocket error code and message */
  public interface StreamSendMapper {

    /**
     * @param streamType type of stream: request or response
     * @param t {@link Throwable} that should be mapped to {@link Error}
     * @return one of stream errors (code and message) according to protocol spec: reject, cancel,
     *     invalid, application. Null if default {@link
     *     ErrorFrameFlyweight#errorCodeFromException(Throwable)} mapping should be applied
     */
    @Nullable
    Error map(StreamType streamType, Throwable t);

    /**
     * Represents one of stream errors according to protocol spec: reject, cancel, invalid,
     * application
     */
    final class Error {
      private final int errorCode;
      private final String message;

      private Error(int errorCode, String message) {
        this.errorCode = errorCode;
        this.message = message;
      }

      public static Error reject(String errorMessage) {
        return new Error(ErrorCodes.REJECTED, errorMessage);
      }

      public static Error cancel(String errorMessage) {
        return new Error(ErrorCodes.CANCELED, errorMessage);
      }

      public static Error invalid(String errorMessage) {
        return new Error(ErrorCodes.INVALID, errorMessage);
      }

      public static Error application(String errorMessage) {
        return new Error(ErrorCodes.APPLICATION_ERROR, errorMessage);
      }

      public String message() {
        return message;
      }

      public int code() {
        return errorCode;
      }
    }
  }

  /** Maps RSocket error code {@link ErrorType} and message to stream {@link Throwable} error */
  public interface StreamReceiveMapper {

    /**
     * @param streamType type of stream: request or response
     * @param errorType one of stream error codes as defined in RSocket spec: reject, cancel,
     *     invalid, application
     * @param errorMessage stream error message
     * @return stream Throwable mapped from error code and error message. Null if default {@link
     *     Exceptions#from(int, String)} should be applied
     */
    @Nullable
    Throwable map(StreamType streamType, ErrorType errorType, String errorMessage);

    enum ErrorType {
      REJECTED,
      CANCELED,
      INVALID,
      APPLICATION;

      static ErrorType fromCode(int code) {
        switch (code) {
          case ErrorCodes.REJECTED:
            return REJECTED;
          case ErrorCodes.CANCELED:
            return CANCELED;
          case ErrorCodes.INVALID:
            return INVALID;
          default:
            return APPLICATION;
        }
      }
    }
  }

  public enum StreamType {
    REQUEST,
    RESPONSE
  }
}
