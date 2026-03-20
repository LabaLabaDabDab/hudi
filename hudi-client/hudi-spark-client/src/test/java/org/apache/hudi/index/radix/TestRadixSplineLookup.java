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

public class TestRadixSplineLookup {

  private static long[] keys(long... values) {
    return values;
  }

  private RadixSplineLookup newLookup(long[] keys) {
    return RadixSplineLookup.build(keys, 2, 4);
  }

  @Test
  public void testExactMatch() {
    RadixSplineLookup lookup = newLookup(keys(10L, 20L, 30L, 40L, 50L));

    LocationLookupResult result = lookup.lookup(30L);

    assertTrue(result.isFound());
    assertEquals(2, result.getPosition());
    assertEquals(2, result.getInsertPosition());
  }

  @Test
  public void testKeyLessThanMinimum() {
    RadixSplineLookup lookup = newLookup(keys(10L, 20L, 30L, 40L, 50L));

    LocationLookupResult result = lookup.lookup(5L);

    assertFalse(result.isFound());
    assertEquals(0, result.getInsertPosition());
  }

  @Test
  public void testKeyGreaterThanMaximum() {
    RadixSplineLookup lookup = newLookup(keys(10L, 20L, 30L, 40L, 50L));

    LocationLookupResult result = lookup.lookup(100L);

    assertFalse(result.isFound());
    assertEquals(5, result.getInsertPosition());
  }

  @Test
  public void testKeyBetweenNeighbors() {
    RadixSplineLookup lookup = newLookup(keys(10L, 20L, 30L, 40L, 50L));

    LocationLookupResult result = lookup.lookup(35L);

    assertFalse(result.isFound());
    assertEquals(3, result.getInsertPosition());
  }

  @Test
  public void testSingleElementSet() {
    RadixSplineLookup lookup = newLookup(keys(42L));

    LocationLookupResult found = lookup.lookup(42L);
    LocationLookupResult notFoundBefore = lookup.lookup(41L);
    LocationLookupResult notFoundAfter = lookup.lookup(43L);

    assertTrue(found.isFound());
    assertEquals(0, found.getPosition());
    assertEquals(0, found.getInsertPosition());

    assertFalse(notFoundBefore.isFound());
    assertEquals(0, notFoundBefore.getInsertPosition());

    assertFalse(notFoundAfter.isFound());
    assertEquals(1, notFoundAfter.getInsertPosition());
  }

  @Test
  public void testTwoElementSet() {
    RadixSplineLookup lookup = newLookup(keys(10L, 20L));

    LocationLookupResult foundLeft = lookup.lookup(10L);
    LocationLookupResult foundRight = lookup.lookup(20L);
    LocationLookupResult missingMiddle = lookup.lookup(15L);

    assertTrue(foundLeft.isFound());
    assertEquals(0, foundLeft.getPosition());
    assertEquals(0, foundLeft.getInsertPosition());

    assertTrue(foundRight.isFound());
    assertEquals(1, foundRight.getPosition());
    assertEquals(1, foundRight.getInsertPosition());

    assertFalse(missingMiddle.isFound());
    assertEquals(1, missingMiddle.getInsertPosition());
  }

  @Test
  public void testDifferentMaxErrorAndRadixBits() {
    long[] keys = keys(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L);

    RadixSplineLookup lookup1 = RadixSplineLookup.build(keys, 1, 2);
    RadixSplineLookup lookup2 = RadixSplineLookup.build(keys, 3, 6);

    LocationLookupResult result1 = lookup1.lookup(60L);
    LocationLookupResult result2 = lookup2.lookup(60L);

    assertTrue(result1.isFound());
    assertEquals(5, result1.getPosition());
    assertEquals(5, result1.getInsertPosition());

    assertTrue(result2.isFound());
    assertEquals(5, result2.getPosition());
    assertEquals(5, result2.getInsertPosition());
  }

  @Test
  public void testLowerBoundInsideRange() {
    long[] keys = keys(10L, 20L, 30L, 40L, 50L);

    int pos = RadixSplineLookup.lowerBound(keys, 35L, 1, 4);

    assertEquals(3, pos);
  }

  @Test
  public void testLowerBoundExactMatch() {
    long[] keys = keys(10L, 20L, 30L, 40L, 50L);

    int pos = RadixSplineLookup.lowerBound(keys, 30L, 0, keys.length);

    assertEquals(2, pos);
  }

  @Test
  public void testLowerBoundAtBeginning() {
    long[] keys = keys(10L, 20L, 30L);

    int pos = RadixSplineLookup.lowerBound(keys, 5L, 0, keys.length);

    assertEquals(0, pos);
  }

  @Test
  public void testLowerBoundAtEnd() {
    long[] keys = keys(10L, 20L, 30L);

    int pos = RadixSplineLookup.lowerBound(keys, 100L, 0, keys.length);

    assertEquals(3, pos);
  }

  @Test
  public void testBuildRejectsNullKeys() {
    assertThrows(NullPointerException.class,
        () -> RadixSplineLookup.build((long[]) null, 2, 4));
  }

  @Test
  public void testBuildRejectsEmptyKeys() {
    assertThrows(IllegalArgumentException.class,
        () -> RadixSplineLookup.build(new long[0], 2, 4));
  }

  @Test
  public void testBuildRejectsNegativeKeyInInput() {
    assertThrows(IllegalArgumentException.class,
        () -> RadixSplineLookup.build(keys(1L, -2L, 3L), 2, 4));
  }

  @Test
  public void testBuildRejectsUnsortedInput() {
    assertThrows(IllegalArgumentException.class,
        () -> RadixSplineLookup.build(keys(10L, 30L, 20L), 2, 4));
  }

  @Test
  public void testBuildRejectsDuplicateKeys() {
    assertThrows(IllegalArgumentException.class,
        () -> RadixSplineLookup.build(keys(10L, 20L, 20L, 30L), 2, 4));
  }

  @Test
  public void testLookupRejectsNegativeKey() {
    RadixSplineLookup lookup = newLookup(keys(10L, 20L, 30L));

    assertThrows(IllegalArgumentException.class, () -> lookup.lookup(-1L));
  }

  @Test
  public void testLowerBoundRejectsNegativeFromIndex() {
    long[] keys = keys(10L, 20L, 30L);

    assertThrows(IllegalArgumentException.class,
        () -> RadixSplineLookup.lowerBound(keys, 20L, -1, 2));
  }

  @Test
  public void testLowerBoundRejectsToIndexLessThanFromIndex() {
    long[] keys = keys(10L, 20L, 30L);

    assertThrows(IllegalArgumentException.class,
        () -> RadixSplineLookup.lowerBound(keys, 20L, 2, 1));
  }

  @Test
  public void testLowerBoundRejectsToIndexGreaterThanSize() {
    long[] keys = keys(10L, 20L, 30L);

    assertThrows(IllegalArgumentException.class,
        () -> RadixSplineLookup.lowerBound(keys, 20L, 0, 4));
  }
}