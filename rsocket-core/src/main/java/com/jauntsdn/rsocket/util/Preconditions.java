package com.jauntsdn.rsocket.util;

import com.jauntsdn.rsocket.frame.FrameLengthFlyweight;

public final class Preconditions {

  public static int assertFrameSizeLimit(int frameSizeLimit) {
    if (frameSizeLimit <= 0) {
      throw new IllegalArgumentException(
          String.format("Frame size limit must be positive: %d", frameSizeLimit));
    }
    int maxFrameSize = FrameLengthFlyweight.FRAME_LENGTH_MASK;
    if (frameSizeLimit > maxFrameSize) {
      throw new IllegalArgumentException(
          String.format(
              "Frame size limit exceeds RSocket max frame size: %d > %d",
              frameSizeLimit, maxFrameSize));
    }
    return frameSizeLimit;
  }
}
