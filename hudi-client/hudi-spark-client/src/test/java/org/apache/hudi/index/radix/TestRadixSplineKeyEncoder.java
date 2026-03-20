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

public class TestRadixSplineKeyEncoder {

  private final RadixSplineKeyEncoder numericEncoder =
      new RadixSplineKeyEncoder(RadixSplineKeyEncoder.Mode.NUMERIC_COLUMN);

  private final RadixSplineKeyEncoder stringDecimalEncoder =
      new RadixSplineKeyEncoder(RadixSplineKeyEncoder.Mode.STRING_DECIMAL_COLUMN);

  @Test
  public void testEncodeNumericKey() {
    long encoded = numericEncoder.encode("12345");
    assertEquals(12345L, encoded);
    assertTrue(encoded >= 0L);
  }

  @Test
  public void testEncodeZero() {
    assertEquals(0L, numericEncoder.encode("0"));
    assertEquals(0L, stringDecimalEncoder.encode("0"));
  }

  @Test
  public void testEncodePreservesOrderForSimpleNumbers() {
    long a = numericEncoder.encode("10");
    long b = numericEncoder.encode("20");
    long c = numericEncoder.encode("30");

    assertTrue(a < b);
    assertTrue(b < c);
  }

  @Test
  public void testStringDecimalModeUsesSameCanonicalEncoding() {
    assertEquals(123L, stringDecimalEncoder.encode("123"));
    assertTrue(stringDecimalEncoder.encode("123") < stringDecimalEncoder.encode("124"));
  }

  @Test
  public void testRejectNullKey() {
    assertThrows(IllegalArgumentException.class, () -> numericEncoder.encode(null));
  }

  @Test
  public void testRejectEmptyKey() {
    assertThrows(IllegalArgumentException.class, () -> numericEncoder.encode(""));
  }

  @Test
  public void testRejectNegativeNumericKey() {
    assertThrows(IllegalArgumentException.class, () -> numericEncoder.encode("-10"));
  }

  @Test
  public void testRejectLeadingPlus() {
    assertThrows(IllegalArgumentException.class, () -> numericEncoder.encode("+10"));
  }

  @Test
  public void testRejectLeadingZeros() {
    assertThrows(IllegalArgumentException.class, () -> numericEncoder.encode("00123"));
    assertThrows(IllegalArgumentException.class, () -> numericEncoder.encode("00"));
  }

  @Test
  public void testAcceptSingleZeroButRejectZeroPrefixedNumbers() {
    assertEquals(0L, numericEncoder.encode("0"));
    assertThrows(IllegalArgumentException.class, () -> numericEncoder.encode("01"));
  }

  @Test
  public void testRejectUnsupportedKeyFormat() {
    assertThrows(IllegalArgumentException.class, () -> numericEncoder.encode("abc"));
    assertThrows(IllegalArgumentException.class, () -> numericEncoder.encode("12a"));
    assertThrows(IllegalArgumentException.class, () -> numericEncoder.encode("1 2"));
  }

  @Test
  public void testRejectOverflow() {
    assertThrows(
        IllegalArgumentException.class,
        () -> numericEncoder.encode("9223372036854775808"));
  }

  @Test
  public void testEncodeLongMaxValue() {
    assertEquals(Long.MAX_VALUE, numericEncoder.encode(Long.toString(Long.MAX_VALUE)));
  }

  @Test
  public void testIsSupportedReturnsTrueForCanonicalDecimal() {
    assertTrue(numericEncoder.isSupported("123"));
    assertTrue(numericEncoder.isSupported("0"));
    assertTrue(stringDecimalEncoder.isSupported("999999"));
  }

  @Test
  public void testIsSupportedReturnsFalseForNonCanonicalValues() {
    assertFalse(numericEncoder.isSupported(null));
    assertFalse(numericEncoder.isSupported(""));
    assertFalse(numericEncoder.isSupported("-1"));
    assertFalse(numericEncoder.isSupported("+1"));
    assertFalse(numericEncoder.isSupported("001"));
    assertFalse(numericEncoder.isSupported("abc"));
  }

  @Test
  public void testDifferentStringRepresentationsDoNotSilentlyCollapse() {
    long canonical = numericEncoder.encode("123");
    assertEquals(123L, canonical);

    assertThrows(IllegalArgumentException.class, () -> numericEncoder.encode("+123"));
    assertThrows(IllegalArgumentException.class, () -> numericEncoder.encode("000123"));
  }
}