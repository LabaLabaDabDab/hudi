/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the License, Version 2.0 (the
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRadixSplineModel {

  @Test
  public void testBuildSingleElementModel() {
    RadixSplineModel model = RadixSplineModel.build(Collections.singletonList(42L), 2, 4);

    assertEquals(1, model.size());

    SearchBound bound = model.getSearchBound(42L);
    assertEquals(0, bound.getLo());
    assertEquals(1, bound.getHi());
  }

  @Test
  public void testBuildTwoElementModel() {
    RadixSplineModel model = RadixSplineModel.build(Arrays.asList(10L, 20L), 2, 4);

    assertEquals(2, model.size());

    SearchBound bound = model.getSearchBound(15L);
    assertTrue(bound.getLo() >= 0);
    assertTrue(bound.getHi() <= 2);
    assertTrue(bound.getHi() >= bound.getLo());
  }

  @Test
  public void testGetSearchBoundForExactMatch() {
    RadixSplineModel model = RadixSplineModel.build(Arrays.asList(10L, 20L, 30L, 40L, 50L), 2, 4);

    SearchBound bound = model.getSearchBound(30L);

    assertTrue(bound.getLo() <= 2);
    assertTrue(bound.getHi() >= 3);
  }

  @Test
  public void testGetSearchBoundForKeyBeforeMinimum() {
    RadixSplineModel model = RadixSplineModel.build(Arrays.asList(10L, 20L, 30L, 40L), 2, 4);

    SearchBound bound = model.getSearchBound(5L);

    assertEquals(0, bound.getLo());
    assertTrue(bound.getHi() > 0);
  }

  @Test
  public void testGetSearchBoundForKeyAfterMaximum() {
    RadixSplineModel model = RadixSplineModel.build(Arrays.asList(10L, 20L, 30L, 40L), 2, 4);

    SearchBound bound = model.getSearchBound(100L);

    assertTrue(bound.getHi() <= 4);
    assertTrue(bound.getLo() >= 0);
    assertTrue(bound.getHi() >= bound.getLo());
  }

  @Test
  public void testGetSearchBoundForKeyBetweenNeighbors() {
    RadixSplineModel model = RadixSplineModel.build(Arrays.asList(10L, 20L, 30L, 40L, 50L), 2, 4);

    SearchBound bound = model.getSearchBound(35L);

    assertTrue(bound.getLo() >= 0);
    assertTrue(bound.getHi() <= 5);
    assertTrue(bound.getHi() >= bound.getLo());
  }

  @Test
  public void testDifferentMaxError() {
    List<Long> keys = Arrays.asList(10L, 20L, 30L, 40L, 50L, 60L, 70L);

    RadixSplineModel model1 = RadixSplineModel.build(keys, 1, 4);
    RadixSplineModel model3 = RadixSplineModel.build(keys, 3, 4);

    SearchBound bound1 = model1.getSearchBound(45L);
    SearchBound bound3 = model3.getSearchBound(45L);

    assertTrue((bound3.getHi() - bound3.getLo()) >= (bound1.getHi() - bound1.getLo()));
  }

  @Test
  public void testDifferentRadixBits() {
    List<Long> keys = Arrays.asList(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L);

    RadixSplineModel model0 = RadixSplineModel.build(keys, 2, 0);
    RadixSplineModel model16 = RadixSplineModel.build(keys, 2, 16);

    SearchBound bound0 = model0.getSearchBound(55L);
    SearchBound bound16 = model16.getSearchBound(55L);

    assertTrue(bound0.getLo() >= 0);
    assertTrue(bound0.getHi() <= keys.size());
    assertTrue(bound16.getLo() >= 0);
    assertTrue(bound16.getHi() <= keys.size());
  }

  @Test
  public void testRejectEmptyKeys() {
    assertThrows(IllegalArgumentException.class,
        () -> RadixSplineModel.build(Collections.emptyList(), 2, 4));
  }

  @Test
  public void testRejectNonIncreasingKeys() {
    assertThrows(
        IllegalArgumentException.class,
        () -> RadixSplineModel.build(Arrays.asList(10L, 20L, 20L, 30L), 2, 4)
    );
  }

  @Test
  public void testRejectNegativeKeyInInput() {
    assertThrows(
        IllegalArgumentException.class,
        () -> RadixSplineModel.build(Arrays.asList(1L, -2L, 3L), 2, 4)
    );
  }

  @Test
  public void testRejectInvalidMaxError() {
    assertThrows(
        IllegalArgumentException.class,
        () -> RadixSplineModel.build(Arrays.asList(1L, 2L, 3L), -1, 4)
    );
  }

  @Test
  public void testRejectInvalidRadixBitsTooSmall() {
    assertThrows(
        IllegalArgumentException.class,
        () -> RadixSplineModel.build(Arrays.asList(10L, 20L, 30L), 2, -1)
    );
  }

  @Test
  public void testRejectInvalidRadixBitsTooLarge() {
    assertThrows(
        IllegalArgumentException.class,
        () -> RadixSplineModel.build(Arrays.asList(10L, 20L, 30L), 2, 21)
    );
  }

  @Test
  public void testBuildRejectsNullKeys() {
    assertThrows(NullPointerException.class,
        () -> RadixSplineModel.build((List<Long>) null, 2, 4));
  }

  @Test
  public void testBuildRejectsNegativeKey() {
    assertThrows(IllegalArgumentException.class,
        () -> RadixSplineModel.build(Arrays.asList(1L, -2L, 3L), 2, 4));
  }

  @Test
  public void testBuildRejectsUnsortedKeys() {
    assertThrows(IllegalArgumentException.class,
        () -> RadixSplineModel.build(Arrays.asList(10L, 30L, 20L), 2, 4));
  }

  @Test
  public void testBuildRejectsDuplicateKeys() {
    assertThrows(IllegalArgumentException.class,
        () -> RadixSplineModel.build(Arrays.asList(10L, 20L, 20L, 30L), 2, 4));
  }

  @Test
  public void testBuildAcceptsZeroMaxError() {
    RadixSplineModel model = RadixSplineModel.build(Arrays.asList(10L, 20L, 30L), 0, 4);
    assertEquals(3, model.size());
  }

  @Test
  public void testBuildAcceptsZeroRadixBits() {
    RadixSplineModel model = RadixSplineModel.build(Arrays.asList(10L, 20L, 30L), 2, 0);
    assertEquals(3, model.size());
  }

  @Test
  public void testBuildAcceptsTwentyRadixBits() {
    RadixSplineModel model = RadixSplineModel.build(Arrays.asList(10L, 20L, 30L), 2, 20);
    assertEquals(3, model.size());
  }

  @Test
  public void testGetSearchBoundRejectsNegativeLookupKey() {
    RadixSplineModel model = RadixSplineModel.build(Arrays.asList(10L, 20L, 30L), 2, 4);

    assertThrows(IllegalArgumentException.class, () -> model.getSearchBound(-1L));
  }

  @Test
  public void testToStringContainsImportantFields() {
    RadixSplineModel model = RadixSplineModel.build(Arrays.asList(10L, 20L, 30L, 40L), 2, 4);

    String s = model.toString();

    assertTrue(s.contains("size"));
    assertTrue(s.contains("splinePoints"));
    assertTrue(s.contains("maxError"));
    assertTrue(s.contains("radixBits"));
  }
}