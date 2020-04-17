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

package com.jauntsdn.rsocket.util;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class NumberUtilsTest {

  @DisplayName("returns int value with postitive int")
  @Test
  void requireNonNegativeInt() {
    assertThat(Preconditions.requireNonNegative(Integer.MAX_VALUE, "test-message"))
        .isEqualTo(Integer.MAX_VALUE);
  }

  @DisplayName(
      "requireNonNegative with int argument throws IllegalArgumentException with negative value")
  @Test
  void requireNonNegativeIntNegative() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Preconditions.requireNonNegative(Integer.MIN_VALUE, "test-message"))
        .withMessage("test-message");
  }

  @DisplayName("requireNonNegative with int argument throws NullPointerException with null message")
  @Test
  void requireNonNegativeIntNullMessage() {
    assertThatNullPointerException()
        .isThrownBy(() -> Preconditions.requireNonNegative(Integer.MIN_VALUE, null))
        .withMessage("message must not be null");
  }

  @DisplayName("requireNonNegative returns int value with zero")
  @Test
  void requireNonNegativeIntZero() {
    assertThat(Preconditions.requireNonNegative(0, "test-message")).isEqualTo(0);
  }

  @DisplayName("requirePositive returns int value with positive int")
  @Test
  void requirePositiveInt() {
    assertThat(Preconditions.requirePositive(Integer.MAX_VALUE, "test-message"))
        .isEqualTo(Integer.MAX_VALUE);
  }

  @DisplayName(
      "requirePositive with int argument throws IllegalArgumentException with negative value")
  @Test
  void requirePositiveIntNegative() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Preconditions.requirePositive(Integer.MIN_VALUE, "test-message"))
        .withMessage("test-message");
  }

  @DisplayName("requirePositive with int argument throws NullPointerException with null message")
  @Test
  void requirePositiveIntNullMessage() {
    assertThatNullPointerException()
        .isThrownBy(() -> Preconditions.requirePositive(Integer.MIN_VALUE, null))
        .withMessage("message must not be null");
  }

  @DisplayName("requirePositive with int argument throws IllegalArgumentException with zero value")
  @Test
  void requirePositiveIntZero() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Preconditions.requirePositive(0, "test-message"))
        .withMessage("test-message");
  }

  @DisplayName("requirePositive returns long value with positive long")
  @Test
  void requirePositiveLong() {
    assertThat(Preconditions.requirePositive(Long.MAX_VALUE, "test-message"))
        .isEqualTo(Long.MAX_VALUE);
  }

  @DisplayName(
      "requirePositive with long argument throws IllegalArgumentException with negative value")
  @Test
  void requirePositiveLongNegative() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Preconditions.requirePositive(Long.MIN_VALUE, "test-message"))
        .withMessage("test-message");
  }

  @DisplayName("requirePositive with long argument throws NullPointerException with null message")
  @Test
  void requirePositiveLongNullMessage() {
    assertThatNullPointerException()
        .isThrownBy(() -> Preconditions.requirePositive(Long.MIN_VALUE, null))
        .withMessage("message must not be null");
  }

  @DisplayName("requirePositive with long argument throws IllegalArgumentException with zero value")
  @Test
  void requirePositiveLongZero() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Preconditions.requirePositive(0L, "test-message"))
        .withMessage("test-message");
  }

  @DisplayName("requireUnsignedByte returns length if 255")
  @Test
  void requireUnsignedByte() {
    assertThat(Preconditions.requireUnsignedByte((1 << 8) - 1)).isEqualTo(255);
  }

  @DisplayName("requireUnsignedByte throws IllegalArgumentException if larger than 255")
  @Test
  void requireUnsignedByteOverFlow() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Preconditions.requireUnsignedByte(1 << 8))
        .withMessage("%d is larger than 8 bits", 1 << 8);
  }

  @DisplayName("requireUnsignedMedium returns length if 16_777_215")
  @Test
  void requireUnsignedMedium() {
    assertThat(Preconditions.requireUnsignedMedium((1 << 24) - 1)).isEqualTo(16_777_215);
  }

  @DisplayName("requireUnsignedMedium throws IllegalArgumentException if larger than 16_777_215")
  @Test
  void requireUnsignedMediumOverFlow() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Preconditions.requireUnsignedMedium(1 << 24))
        .withMessage("%d is larger than 24 bits", 1 << 24);
  }

  @DisplayName("requireUnsignedShort returns length if 65_535")
  @Test
  void requireUnsignedShort() {
    assertThat(Preconditions.requireUnsignedShort((1 << 16) - 1)).isEqualTo(65_535);
  }

  @DisplayName("requireUnsignedShort throws IllegalArgumentException if larger than 65_535")
  @Test
  void requireUnsignedShortOverFlow() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Preconditions.requireUnsignedShort(1 << 16))
        .withMessage("%d is larger than 16 bits", 1 << 16);
  }
}
