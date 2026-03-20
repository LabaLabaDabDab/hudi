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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Radix Spline model for monotonic sorted non-negative keys.
 *
 * Search bounds returned by {@link #getSearchBound(long)} are in [lo, hi) form,
 * where hi is exclusive.
 */
final class RadixSplineModel implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int size;
  private final int maxError;
  private final int radixBits;

  private final long minKey;
  private final long maxKey;

  /** Spline points: x = splineKeys[i], y = splinePositions[i]. */
  private final long[] splineKeys;
  private final int[] splinePositions;

  /** Radix directory over spline points. */
  private final int[] radixMinIndex;
  private final int[] radixMaxIndex;

  private RadixSplineModel(
      int size,
      int maxError,
      int radixBits,
      long minKey,
      long maxKey,
      long[] splineKeys,
      int[] splinePositions,
      int[] radixMinIndex,
      int[] radixMaxIndex) {

    this.splineKeys = Objects.requireNonNull(splineKeys, "splineKeys must not be null");
    this.splinePositions = Objects.requireNonNull(splinePositions, "splinePositions must not be null");
    this.radixMinIndex = Objects.requireNonNull(radixMinIndex, "radixMinIndex must not be null");
    this.radixMaxIndex = Objects.requireNonNull(radixMaxIndex, "radixMaxIndex must not be null");

    if (size <= 0) {
      throw new IllegalArgumentException("size must be > 0");
    }
    if (splineKeys.length == 0) {
      throw new IllegalArgumentException("splineKeys must be non-empty");
    }
    if (splineKeys.length != splinePositions.length) {
      throw new IllegalArgumentException("splineKeys and splinePositions sizes must match");
    }

    this.size = size;
    this.maxError = maxError;
    this.radixBits = radixBits;
    this.minKey = minKey;
    this.maxKey = maxKey;
  }

  static RadixSplineModel build(List<Long> sortedKeys, int maxError, int radixBits) {
    Objects.requireNonNull(sortedKeys, "sortedKeys must not be null");
    return build(toLongArray(sortedKeys), maxError, radixBits);
  }

  static RadixSplineModel build(long[] sortedKeys, int maxError, int radixBits) {
    validateInputs(sortedKeys, maxError, radixBits);

    SplineData splineData = buildSplinePoints(sortedKeys, maxError);
    RadixDirectory radixDirectory = buildRadixDirectory(splineData.keys, radixBits);

    return new RadixSplineModel(
        sortedKeys.length,
        maxError,
        radixBits,
        sortedKeys[0],
        sortedKeys[sortedKeys.length - 1],
        splineData.keys,
        splineData.positions,
        radixDirectory.minIndex,
        radixDirectory.maxIndex
    );
  }

  int size() {
    return size;
  }

  SearchBound getSearchBound(long key) {
    if (key < 0) {
      throw new IllegalArgumentException("key must be >= 0");
    }

    if (size == 1) {
      return new SearchBound(0, 1);
    }

    if (key <= minKey) {
      return new SearchBound(0, Math.min(size, maxError + 1));
    }

    if (key >= maxKey) {
      return new SearchBound(Math.max(0, size - (maxError + 1)), size);
    }

    int[] slice = candidateSplineSlice(key);
    int sliceFrom = slice[0];
    int sliceTo = slice[1]; // inclusive

    int idx = lowerBound(splineKeys, key, sliceFrom, sliceTo + 1); // hi exclusive

    int leftIdx;
    int rightIdx;

    if (idx <= sliceFrom) {
      leftIdx = sliceFrom;
      rightIdx = Math.min(sliceFrom + 1, splineKeys.length - 1);
    } else if (idx > sliceTo) {
      rightIdx = sliceTo;
      leftIdx = Math.max(sliceTo - 1, 0);
    } else {
      rightIdx = idx;
      leftIdx = idx - 1;
    }

    long x0 = splineKeys[leftIdx];
    long x1 = splineKeys[rightIdx];
    int y0 = splinePositions[leftIdx];
    int y1 = splinePositions[rightIdx];

    int estimatedPos = interpolatePosition(key, x0, y0, x1, y1);

    int lo = Math.max(0, estimatedPos - maxError);
    int hi = Math.min(size, estimatedPos + maxError + 1);

    // Guarantee anchors are inside the returned interval
    lo = Math.min(lo, Math.min(y0, y1));
    hi = Math.max(hi, Math.max(y0, y1) + 1);

    lo = Math.max(0, lo);
    hi = Math.min(size, hi);

    if (lo >= hi) {
      lo = Math.max(0, Math.min(size - 1, estimatedPos));
      hi = Math.min(size, lo + 1);
    }

    return new SearchBound(lo, hi);
  }

  private int[] candidateSplineSlice(long key) {
    if (splineKeys.length == 1 || radixBits == 0) {
      return new int[] {0, splineKeys.length - 1};
    }

    int prefix = radixPrefix(key, radixBits);

    int lo = radixMinIndex[prefix];
    int hi = radixMaxIndex[prefix];

    if (lo < 0 || hi < 0) {
      return new int[] {0, splineKeys.length - 1};
    }

    lo = Math.max(0, lo);
    hi = Math.min(splineKeys.length - 1, hi);

    if (lo > hi) {
      return new int[] {0, splineKeys.length - 1};
    }

    return new int[] {lo, hi};
  }

  private static int interpolatePosition(long key, long x0, int y0, long x1, int y1) {
    if (x1 == x0) {
      return y0;
    }

    double ratio = (double) (key - x0) / (double) (x1 - x0);
    double estimate = y0 + ratio * (y1 - y0);

    if (estimate <= Integer.MIN_VALUE) {
      return Integer.MIN_VALUE;
    }
    if (estimate >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) estimate;
  }

  private static int lowerBound(long[] keys, long key, int fromIndex, int toIndex) {
    if (fromIndex < 0) {
      throw new IllegalArgumentException("fromIndex must be >= 0");
    }
    if (toIndex < fromIndex) {
      throw new IllegalArgumentException("toIndex must be >= fromIndex");
    }
    if (toIndex > keys.length) {
      throw new IllegalArgumentException("toIndex must be <= keys.length");
    }

    int lo = fromIndex;
    int hi = toIndex;

    while (lo < hi) {
      int mid = lo + ((hi - lo) >>> 1);
      if (keys[mid] < key) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    return lo;
  }

  private static double slope(long x0, int y0, long x1, int y1) {
    if (x1 <= x0) {
      throw new IllegalArgumentException("x1 must be > x0");
    }
    return ((double) y1 - (double) y0) / ((double) x1 - (double) x0);
  }

  private static SplineData buildSplinePoints(long[] keys, int maxError) {
    if (keys.length == 1) {
      return new SplineData(
          new long[] {keys[0]},
          new int[] {0}
      );
    }

    LongArrayBuilder splineKeys = new LongArrayBuilder(Math.min(keys.length, 1024));
    IntArrayBuilder splinePositions = new IntArrayBuilder(Math.min(keys.length, 1024));

    splineKeys.add(keys[0]);
    splinePositions.add(0);

    int basePos = 0;
    long baseKey = keys[0];

    double pMin = Double.NEGATIVE_INFINITY;
    double pMax = Double.POSITIVE_INFINITY;

    for (int i = 1; i < keys.length; i++) {
      long key = keys[i];
      long dx = key - baseKey;

      if (dx <= 0) {
        throw new IllegalArgumentException("sortedKeys must be strictly increasing");
      }

      double localMin = slope(baseKey, basePos, key, i - maxError);
      double localMax = slope(baseKey, basePos, key, i + maxError);

      double nextPMin = Math.max(pMin, localMin);
      double nextPMax = Math.min(pMax, localMax);

      if (nextPMin > nextPMax) {
        int prevPos = i - 1;

        if (splinePositions.getLast() != prevPos) {
          splineKeys.add(keys[prevPos]);
          splinePositions.add(prevPos);
        }

        basePos = prevPos;
        baseKey = keys[basePos];

        pMin = Double.NEGATIVE_INFINITY;
        pMax = Double.POSITIVE_INFINITY;

        i = prevPos;
        continue;
      }

      pMin = nextPMin;
      pMax = nextPMax;
    }

    int lastPos = keys.length - 1;
    if (splinePositions.getLast() != lastPos) {
      splineKeys.add(keys[lastPos]);
      splinePositions.add(lastPos);
    }

    return new SplineData(splineKeys.toArray(), splinePositions.toArray());
  }

  private static RadixDirectory buildRadixDirectory(long[] splineKeys, int radixBits) {
    if (radixBits < 0 || radixBits > 20) {
      throw new IllegalArgumentException("radixBits must be in [0, 20]");
    }

    int prefixCount = radixBits == 0 ? 1 : (1 << radixBits);
    int[] minIndex = new int[prefixCount];
    int[] maxIndex = new int[prefixCount];

    Arrays.fill(minIndex, -1);
    Arrays.fill(maxIndex, -1);

    for (int i = 0; i < splineKeys.length; i++) {
      int prefix = radixPrefix(splineKeys[i], radixBits);
      if (minIndex[prefix] < 0) {
        minIndex[prefix] = i;
      }
      maxIndex[prefix] = i;
    }

    for (int prefix = 0; prefix < prefixCount; prefix++) {
      if (minIndex[prefix] >= 0) {
        minIndex[prefix] = Math.max(0, minIndex[prefix] - 1);
        maxIndex[prefix] = Math.min(splineKeys.length - 1, maxIndex[prefix] + 1);
      }
    }

    return new RadixDirectory(minIndex, maxIndex);
  }

  private static int radixPrefix(long key, int radixBits) {
    if (radixBits == 0) {
      return 0;
    }
    return (int) (key >>> (64 - radixBits));
  }

  private static void validateInputs(long[] sortedKeys, int maxError, int radixBits) {
    Objects.requireNonNull(sortedKeys, "sortedKeys must not be null");

    if (sortedKeys.length == 0) {
      throw new IllegalArgumentException("sortedKeys must be non-empty");
    }
    if (maxError < 0) {
      throw new IllegalArgumentException("maxError must be >= 0");
    }
    if (radixBits < 0 || radixBits > 20) {
      throw new IllegalArgumentException("radixBits must be in [0, 20]");
    }

    long prev = -1L;
    for (int i = 0; i < sortedKeys.length; i++) {
      long key = sortedKeys[i];
      if (key < 0) {
        throw new IllegalArgumentException("sortedKeys must contain only non-negative values");
      }
      if (i > 0 && key <= prev) {
        throw new IllegalArgumentException("sortedKeys must be strictly increasing");
      }
      prev = key;
    }
  }

  private static long[] toLongArray(List<Long> sortedKeys) {
    long[] result = new long[sortedKeys.size()];
    for (int i = 0; i < sortedKeys.size(); i++) {
      Long value = sortedKeys.get(i);
      if (value == null) {
        throw new IllegalArgumentException("sortedKeys must not contain null");
      }
      result[i] = value;
    }
    return result;
  }

  @Override
  public String toString() {
    return "RadixSplineModel{"
        + "size=" + size
        + ", maxError=" + maxError
        + ", radixBits=" + radixBits
        + ", minKey=" + minKey
        + ", maxKey=" + maxKey
        + ", splinePoints=" + splineKeys.length
        + '}';
  }

  private static final class SplineData implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long[] keys;
    private final int[] positions;

    private SplineData(long[] keys, int[] positions) {
      this.keys = keys;
      this.positions = positions;
    }
  }

  private static final class RadixDirectory implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int[] minIndex;
    private final int[] maxIndex;

    private RadixDirectory(int[] minIndex, int[] maxIndex) {
      this.minIndex = minIndex;
      this.maxIndex = maxIndex;
    }
  }

  private static final class LongArrayBuilder implements Serializable {
    private static final long serialVersionUID = 1L;

    private long[] data;
    private int size;

    private LongArrayBuilder(int initialCapacity) {
      this.data = new long[Math.max(1, initialCapacity)];
      this.size = 0;
    }

    void add(long value) {
      ensureCapacity(size + 1);
      data[size++] = value;
    }

    long[] toArray() {
      return Arrays.copyOf(data, size);
    }

    private void ensureCapacity(int targetSize) {
      if (targetSize <= data.length) {
        return;
      }
      int newCapacity = Math.max(targetSize, data.length << 1);
      data = Arrays.copyOf(data, newCapacity);
    }
  }

  private static final class IntArrayBuilder implements Serializable {
    private static final long serialVersionUID = 1L;

    private int[] data;
    private int size;

    private IntArrayBuilder(int initialCapacity) {
      this.data = new int[Math.max(1, initialCapacity)];
      this.size = 0;
    }

    void add(int value) {
      ensureCapacity(size + 1);
      data[size++] = value;
    }

    int getLast() {
      if (size == 0) {
        throw new IllegalStateException("builder is empty");
      }
      return data[size - 1];
    }

    int[] toArray() {
      return Arrays.copyOf(data, size);
    }

    private void ensureCapacity(int targetSize) {
      if (targetSize <= data.length) {
        return;
      }
      int newCapacity = Math.max(targetSize, data.length << 1);
      data = Arrays.copyOf(data, newCapacity);
    }
  }
}
