/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.index.radix;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRadixSplineKeyEncoder {

  @Test
  void encodeAcceptsZero() {
    RadixSplineKeyEncoder encoder =
        new RadixSplineKeyEncoder(RadixSplineKeyEncoder.Mode.STRING_DECIMAL_COLUMN);

    assertEquals(0L, encoder.encode("0"));
  }

  @Test
  void encodeAcceptsCanonicalDecimal() {
    RadixSplineKeyEncoder encoder =
        new RadixSplineKeyEncoder(RadixSplineKeyEncoder.Mode.STRING_DECIMAL_COLUMN);

    assertEquals(123456789L, encoder.encode("123456789"));
  }

  @Test
  void encodeRejectsNull() {
    RadixSplineKeyEncoder encoder =
        new RadixSplineKeyEncoder(RadixSplineKeyEncoder.Mode.STRING_DECIMAL_COLUMN);

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> encoder.encode(null));
    assertTrue(ex.getMessage().contains("must not be null"));
  }

  @Test
  void encodeRejectsEmpty() {
    RadixSplineKeyEncoder encoder =
        new RadixSplineKeyEncoder(RadixSplineKeyEncoder.Mode.STRING_DECIMAL_COLUMN);

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> encoder.encode(""));
    assertTrue(ex.getMessage().contains("must not be empty"));
  }

  @Test
  void encodeRejectsLeadingPlus() {
    RadixSplineKeyEncoder encoder =
        new RadixSplineKeyEncoder(RadixSplineKeyEncoder.Mode.STRING_DECIMAL_COLUMN);

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> encoder.encode("+1"));
    assertTrue(ex.getMessage().contains("leading '+'"));
  }

  @Test
  void encodeRejectsNegative() {
    RadixSplineKeyEncoder encoder =
        new RadixSplineKeyEncoder(RadixSplineKeyEncoder.Mode.STRING_DECIMAL_COLUMN);

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> encoder.encode("-1"));
    assertTrue(ex.getMessage().contains("negative"));
  }

  @Test
  void encodeRejectsLeadingZeros() {
    RadixSplineKeyEncoder encoder =
        new RadixSplineKeyEncoder(RadixSplineKeyEncoder.Mode.STRING_DECIMAL_COLUMN);

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> encoder.encode("001"));
    assertTrue(ex.getMessage().contains("leading zeros"));
  }

  @Test
  void encodeRejectsNonNumeric() {
    RadixSplineKeyEncoder encoder =
        new RadixSplineKeyEncoder(RadixSplineKeyEncoder.Mode.STRING_DECIMAL_COLUMN);

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> encoder.encode("12a3"));
    assertTrue(ex.getMessage().contains("non-numeric"));
  }

  @Test
  void encodeRejectsOverflow() {
    RadixSplineKeyEncoder encoder =
        new RadixSplineKeyEncoder(RadixSplineKeyEncoder.Mode.STRING_DECIMAL_COLUMN);

    String overflow = String.valueOf(Long.MAX_VALUE) + "0";

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> encoder.encode(overflow));
    assertTrue(ex.getMessage().contains("Long.MAX_VALUE"));
  }

  @Test
  void isSupportedReflectsValidity() {
    RadixSplineKeyEncoder encoder =
        new RadixSplineKeyEncoder(RadixSplineKeyEncoder.Mode.STRING_DECIMAL_COLUMN);

    assertTrue(encoder.isSupported("42"));
    assertFalse(encoder.isSupported("0042"));
    assertFalse(encoder.isSupported("-42"));
    assertFalse(encoder.isSupported("abc"));
  }
}