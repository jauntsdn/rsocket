package com.jauntsdn.rsocket.util;

import com.jauntsdn.rsocket.frame.FrameLengthFlyweight;
import java.util.Objects;

public final class Preconditions {

  /** The size of a medium in {@code byte}s. */
  public static final int MEDIUM_BYTES = 3;

  private static final int UNSIGNED_BYTE_SIZE = 8;
  private static final int UNSIGNED_BYTE_MAX_VALUE = (1 << UNSIGNED_BYTE_SIZE) - 1;
  private static final int UNSIGNED_MEDIUM_SIZE = 24;
  private static final int UNSIGNED_MEDIUM_MAX_VALUE = (1 << UNSIGNED_MEDIUM_SIZE) - 1;
  private static final int UNSIGNED_SHORT_SIZE = 16;
  private static final int UNSIGNED_SHORT_MAX_VALUE = (1 << UNSIGNED_SHORT_SIZE) - 1;

  public static int requireFrameSizeValid(int frameSizeLimit) {
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

  /**
   * Requires that an {@code int} is greater than or equal to zero.
   *
   * @param i the {@code int} to test
   * @param message detail message to be used in the event that a {@link IllegalArgumentException}
   *     is thrown
   * @return the {@code int} if greater than or equal to zero
   * @throws IllegalArgumentException if {@code i} is less than zero
   */
  public static int requireNonNegative(int i, String message) {
    Objects.requireNonNull(message, "message must not be null");

    if (i < 0) {
      throw new IllegalArgumentException(message);
    }

    return i;
  }

  /**
   * Requires that a {@code long} is greater than zero.
   *
   * @param l the {@code long} to test
   * @param message detail message to be used in the event that a {@link IllegalArgumentException}
   *     is thrown
   * @return the {@code long} if greater than zero
   * @throws IllegalArgumentException if {@code l} is less than or equal to zero
   */
  public static long requirePositive(long l, String message) {
    Objects.requireNonNull(message, "message must not be null");

    if (l <= 0) {
      throw new IllegalArgumentException(message);
    }

    return l;
  }

  /**
   * Requires that an {@code int} is greater than zero.
   *
   * @param i the {@code int} to test
   * @param message detail message to be used in the event that a {@link IllegalArgumentException}
   *     is thrown
   * @return the {@code int} if greater than zero
   * @throws IllegalArgumentException if {@code i} is less than or equal to zero
   */
  public static int requirePositive(int i, String message) {
    Objects.requireNonNull(message, "message must not be null");

    if (i <= 0) {
      throw new IllegalArgumentException(message);
    }

    return i;
  }

  /**
   * Requires that an {@code int} can be represented as an unsigned {@code byte}.
   *
   * @param i the {@code int} to test
   * @return the {@code int} if it can be represented as an unsigned {@code byte}
   * @throws IllegalArgumentException if {@code i} cannot be represented as an unsigned {@code byte}
   */
  public static int requireUnsignedByte(int i) {
    if (i > UNSIGNED_BYTE_MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format("%d is larger than %d bits", i, UNSIGNED_BYTE_SIZE));
    }

    return i;
  }

  /**
   * Requires that an {@code int} can be represented as an unsigned {@code medium}.
   *
   * @param i the {@code int} to test
   * @return the {@code int} if it can be represented as an unsigned {@code medium}
   * @throws IllegalArgumentException if {@code i} cannot be represented as an unsigned {@code
   *     medium}
   */
  public static int requireUnsignedMedium(int i) {
    if (i > UNSIGNED_MEDIUM_MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format("%d is larger than %d bits", i, UNSIGNED_MEDIUM_SIZE));
    }

    return i;
  }

  /**
   * Requires that an {@code int} can be represented as an unsigned {@code short}.
   *
   * @param i the {@code int} to test
   * @return the {@code int} if it can be represented as an unsigned {@code short}
   * @throws IllegalArgumentException if {@code i} cannot be represented as an unsigned {@code
   *     short}
   */
  public static int requireUnsignedShort(int i) {
    if (i > UNSIGNED_SHORT_MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format("%d is larger than %d bits", i, UNSIGNED_SHORT_SIZE));
    }

    return i;
  }
}
