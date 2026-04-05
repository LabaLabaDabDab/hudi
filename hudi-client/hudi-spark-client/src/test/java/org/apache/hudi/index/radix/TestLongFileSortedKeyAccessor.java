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
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestLongFileSortedKeyAccessor {

  @TempDir Path tempDir;

  @Test
  public void testBuildMatchesInMemoryAcrossWindowBoundary() throws IOException {
    int n = 5000;
    long[] keys = new long[n];
    for (int i = 0; i < n; i++) {
      keys[i] = (long) i * 3L + 1L;
    }

    Path keysFile = tempDir.resolve("keys.bin");
    try (DataOutputStream out =
             new DataOutputStream(Files.newOutputStream(keysFile))) {
      for (long k : keys) {
        out.writeLong(k);
      }
    }

    int maxError = 4;
    int radixBits = 6;

    RadixSplineModel fromArray = RadixSplineModel.build(keys, maxError, radixBits);

    RadixSplineModel fromFile;
    try (LongFileSortedKeyAccessor accessor = new LongFileSortedKeyAccessor(keysFile, n)) {
      fromFile = RadixSplineModel.build(accessor, maxError, radixBits);
    }

    assertRadixSplineModelsEqual(fromArray, fromFile);
  }

  @Test
  public void testBuildMatchesInMemorySmall() throws IOException {
    long[] keys = new long[] {2L, 5L, 9L, 100L, 200L};
    Path keysFile = tempDir.resolve("keys-small.bin");
    try (DataOutputStream out =
             new DataOutputStream(Files.newOutputStream(keysFile))) {
      for (long k : keys) {
        out.writeLong(k);
      }
    }

    int maxError = 2;
    int radixBits = 4;

    RadixSplineModel fromArray = RadixSplineModel.build(keys, maxError, radixBits);

    RadixSplineModel fromFile;
    try (LongFileSortedKeyAccessor accessor = new LongFileSortedKeyAccessor(keysFile, keys.length)) {
      fromFile = RadixSplineModel.build(accessor, maxError, radixBits);
    }

    assertRadixSplineModelsEqual(fromArray, fromFile);
  }

  private static void assertRadixSplineModelsEqual(RadixSplineModel a, RadixSplineModel b) {
    assertEquals(a.size(), b.size());
    assertEquals(a.maxError(), b.maxError());
    assertEquals(a.radixBits(), b.radixBits());
    assertEquals(a.minKey(), b.minKey());
    assertEquals(a.maxKey(), b.maxKey());
    assertArrayEquals(a.splineKeys(), b.splineKeys());
    assertArrayEquals(a.splinePositions(), b.splinePositions());
    assertArrayEquals(a.radixMinIndex(), b.radixMinIndex());
    assertArrayEquals(a.radixMaxIndex(), b.radixMaxIndex());
  }
}
