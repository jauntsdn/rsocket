/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jauntsdn.rsocket.exceptions;

import com.jauntsdn.rsocket.frame.ErrorFrameFlyweight;
import io.netty.buffer.ByteBuf;
import java.util.Objects;

/** Utility class that generates an exception from a frame. */
public final class Exceptions {

  private Exceptions() {}

  /**
   * Create a {@link RSocketException} from a Frame that matches the error code it contains.
   *
   * @param frame the frame to retrieve the error code and message from
   * @return a {@link RSocketException} that matches the error code in the Frame
   * @throws NullPointerException if {@code frame} is {@code null}
   */
  public static RuntimeException from(ByteBuf frame) {
    Objects.requireNonNull(frame, "frame must not be null");
    int errorCode = ErrorFrameFlyweight.errorCode(frame);
    String message = ErrorFrameFlyweight.dataUtf8(frame);
    return from(errorCode, message);
  }

  public static RuntimeException from(int errorCode, String message) {
    switch (errorCode) {
      case ErrorFrameFlyweight.APPLICATION_ERROR:
        return new ApplicationErrorException(message);
      case ErrorFrameFlyweight.CANCELED:
        return new CanceledException(message);
      case ErrorFrameFlyweight.CONNECTION_CLOSE:
        return new ConnectionCloseException(message);
      case ErrorFrameFlyweight.CONNECTION_ERROR:
        return new ConnectionErrorException(message);
      case ErrorFrameFlyweight.INVALID:
        return new InvalidException(message);
      case ErrorFrameFlyweight.INVALID_SETUP:
        return new InvalidSetupException(message);
      case ErrorFrameFlyweight.REJECTED:
        return new RejectedException(message);
      case ErrorFrameFlyweight.REJECTED_RESUME:
        return new RejectedResumeException(message);
      case ErrorFrameFlyweight.REJECTED_SETUP:
        return new RejectedSetupException(message);
      case ErrorFrameFlyweight.UNSUPPORTED_SETUP:
        return new UnsupportedSetupException(message);
      default:
        return new IllegalArgumentException(
            String.format("Invalid Error frame: %d '%s'", errorCode, message));
    }
  }

  public static final String LEASE_EXPIRED_MESSAGE = "lease_expired";
  public static final String LEASE_EXHAUSTED_MESSAGE = "lease_exhausted";

  public static final RejectedException LEASE_EXPIRED_EXCEPTION =
      new RejectedException(LEASE_EXPIRED_MESSAGE);
  public static final RejectedException LEASE_EXHAUSTED_EXCEPTION =
      new RejectedException(LEASE_EXHAUSTED_MESSAGE);
}
