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

package com.jauntsdn.rsocket.internal;

import com.jauntsdn.rsocket.DuplexConnection;
import com.jauntsdn.rsocket.exceptions.ConnectionErrorException;
import com.jauntsdn.rsocket.exceptions.InvalidSetupException;
import com.jauntsdn.rsocket.frame.ErrorCodes;
import com.jauntsdn.rsocket.frame.ErrorFrameFlyweight;
import com.jauntsdn.rsocket.frame.FrameHeaderFlyweight;
import com.jauntsdn.rsocket.frame.FrameType;
import com.jauntsdn.rsocket.util.DuplexConnectionProxy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Checks RSocket state transitions are according to spec. Validates incoming frames type */
public abstract class ValidatingConnection extends DuplexConnectionProxy {
  Start state;

  public static ValidatingConnection ofServer(DuplexConnection connection) {
    return new Server(connection);
  }

  public static ValidatingConnection ofClient(DuplexConnection connection) {
    return new Client(connection, false);
  }

  public static ValidatingConnection ofResumedClient(DuplexConnection connection) {
    return new Client(connection, true);
  }

  ValidatingConnection(DuplexConnection connection) {
    super(connection);
  }

  @Override
  public Flux<ByteBuf> receive() {
    return super.receive()
        .handle(
            (frame, sink) -> {
              FrameType frameType = FrameHeaderFlyweight.frameType(frame);

              /*unknown frame*/
              if (frameType == null) {
                int frameTypeCode = FrameHeaderFlyweight.frameTypeCode(frame);
                frame.release();
                sink.error(
                    new IllegalArgumentException(
                        String.format("Unknown incoming frame type: %d", frameTypeCode)));
                return;
              }
              /*no frames received yet*/
              if (state == null) {
                int streamId = FrameHeaderFlyweight.streamId(frame);
                Start newState = onStartFrame(streamId, frameType);

                if (newState != Start.ERROR) {
                  state = newState;
                  /*invalid start*/
                } else {
                  frame.release();
                  InvalidSetupException invalidSetup =
                      new InvalidSetupException(
                          String.format(
                              "Unexpected start frame: streamId %s, frameType %s",
                              streamId, frameType));
                  sendErrorFrame(invalidSetup);
                  sink.error(invalidSetup);
                  return;
                }
                /*started*/
              } else {
                Duplicate duplicate = checkDuplicateStartFrame(frame, frameType);
                if (duplicate != Duplicate.PROCEED) {
                  frame.release();
                  if (duplicate == Duplicate.ERROR) {
                    ConnectionErrorException connectionError =
                        new ConnectionErrorException(
                            String.format(
                                "Unexpected duplicated setup frame: frameType %s", frameType));
                    sendErrorFrame(connectionError);
                    sink.error(connectionError);
                  }
                  return;
                }
                if (frameType == FrameType.METADATA_PUSH) {
                  if (FrameHeaderFlyweight.streamId(frame) != 0) {
                    frame.release();
                    return;
                  }
                } else if (frameType == FrameType.ERROR
                    && FrameHeaderFlyweight.streamId(frame) == 0) {
                  switch (ErrorFrameFlyweight.errorCode(frame)) {
                    case ErrorCodes.INVALID_SETUP:
                    case ErrorCodes.UNSUPPORTED_SETUP:
                    case ErrorCodes.REJECTED_SETUP:
                    case ErrorCodes.REJECTED_RESUME:
                      frame.release();
                      return;
                    default:
                      // noop
                  }
                }
              }
              sink.next(frame);
            });
  }

  abstract Start onStartFrame(int streamId, FrameType frameType);

  abstract Duplicate checkDuplicateStartFrame(ByteBuf frame, FrameType frameType);

  private void sendErrorFrame(Exception e) {
    ByteBuf errorFrame = ErrorFrameFlyweight.encode(ByteBufAllocator.DEFAULT, 0, e);
    sendOne(errorFrame).onErrorResume(err -> Mono.empty()).subscribe();
  }

  private enum Duplicate {
    ERROR,
    FILTER,
    PROCEED
  }

  private enum Start {
    ERROR,
    SETUP,
    RESUME
  }

  static final class Server extends ValidatingConnection {

    Server(DuplexConnection connection) {
      super(connection);
    }

    @Override
    Start onStartFrame(int streamId, FrameType frameType) {
      if (streamId != 0) {
        return Start.ERROR;
      }
      switch (frameType) {
        case SETUP:
          return Start.SETUP;
        case RESUME:
          return Start.RESUME;
        default:
          return Start.ERROR;
      }
    }

    @Override
    Duplicate checkDuplicateStartFrame(ByteBuf frame, FrameType frameType) {
      if (state == Start.SETUP) {
        switch (frameType) {
          case SETUP:
            return Duplicate.FILTER;
          case RESUME:
            return Duplicate.ERROR;
          default:
            // noop
        }
        /*resume*/
      } else {
        switch (frameType) {
          case SETUP:
          case RESUME:
            return Duplicate.ERROR;
          default:
            // noop
        }
      }
      return Duplicate.PROCEED;
    }
  }

  static final class Client extends ValidatingConnection {
    private final boolean isResume;

    Client(DuplexConnection connection, boolean isResume) {
      super(connection);
      this.isResume = isResume;
    }

    @Override
    Start onStartFrame(int streamId, FrameType frameType) {
      if (!isResume) {
        return Start.SETUP;
      }
      /*resume*/
      if (streamId != 0) {
        return Start.ERROR;
      }
      switch (frameType) {
        case RESUME_OK:
        case ERROR:
          return Start.RESUME;
        default:
          return Start.ERROR;
      }
    }

    @Override
    Duplicate checkDuplicateStartFrame(ByteBuf frame, FrameType frameType) {
      switch (frameType) {
        case SETUP:
        case RESUME:
          return Duplicate.FILTER;
        default:
          // noop
      }
      return Duplicate.PROCEED;
    }
  }
}
