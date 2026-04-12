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
import java.util.Objects;

/**
 * Exact lookup helper built on top of {@link RadixSplineModel}.
 *
 * <p>This class is storage-agnostic: it performs exact search through
 * {@link SortedKeyAccessor}, which allows using either in-memory arrays
 * or file-backed key readers.
 */
final class RadixSplineLookup implements Serializable {

  private static final long serialVersionUID = 1L;

  private final SortedKeyAccessor keyAccessor;
  private final RadixSplineModel model;

  private RadixSplineLookup(SortedKeyAccessor keyAccessor, RadixSplineModel model) {
    this.keyAccessor = Objects.requireNonNull(keyAccessor, "keyAccessor must not be null");
    this.model = Objects.requireNonNull(model, "model must not be null");

    if (keyAccessor.size() == 0) {
      throw new IllegalArgumentException("keyAccessor must be non-empty");
    }
    if (keyAccessor.size() != model.size()) {
      throw new IllegalArgumentException(
          "keyAccessor size must match model size: keys=" + keyAccessor.size()
              + ", model=" + model.size());
    }
  }

  static RadixSplineLookup build(long[] sortedKeys, int maxError, int radixBits) {
    return build(new InMemoryKeyAccessor(sortedKeys), maxError, radixBits);
  }

  static RadixSplineLookup build(SortedKeyAccessor keyAccessor, int maxError, int radixBits) {
    validateSortedKeys(keyAccessor);
    return new RadixSplineLookup(
        keyAccessor,
        RadixSplineModel.build(keyAccessor, maxError, radixBits));
  }

  static RadixSplineLookup fromModel(SortedKeyAccessor keyAccessor, RadixSplineModel model) {
    validateSortedKeys(keyAccessor);
    return new RadixSplineLookup(keyAccessor, model);
  }

  RadixSplineModel getModel() {
    return model;
  }

  SortedKeyAccessor getKeyAccessor() {
    return keyAccessor;
  }

  LocationLookupResult lookup(long key) {
    if (key < 0) {
      throw new IllegalArgumentException("key must be >= 0");
    }

    SearchBound bound = model.getSearchBound(key);
    int pos = lowerBound(keyAccessor, key, bound.getLo(), bound.getHi());

    if (pos < keyAccessor.size() && keyAccessor.keyAt(pos) == key) {
      return LocationLookupResult.found(pos, bound);
    }

    return LocationLookupResult.notFound(pos, bound);
  }

  LocationLookupResult lookupWithTiming(long key, LookupTiming timing) {
    Objects.requireNonNull(timing, "timing must not be null");
    if (key < 0) {
      throw new IllegalArgumentException("key must be >= 0");
    }

    SearchBound bound = model.getSearchBound(key);
    int width = bound.getHi() - bound.getLo();
    if (width <= 0) {
      timing.recordBoundWidth(0);
      timing.recordBucket(false, true);
      return LocationLookupResult.notFound(bound.getLo(), bound);
    }

    timing.recordBoundWidth(width);
    int pos = lowerBoundWithTiming(keyAccessor, key, bound.getLo(), bound.getHi(), timing);

    if (pos < keyAccessor.size()) {
      timing.recordKeyAtCall();
      if (keyAccessor.keyAt(pos) == key) {
        timing.recordBucket(true, false);
        return LocationLookupResult.found(pos, bound);
      }
    }

    timing.recordBucket(false, false);
    return LocationLookupResult.notFound(pos, bound);
  }

  static int lowerBound(SortedKeyAccessor keys, long key, int fromIndex, int toIndex) {
    Objects.requireNonNull(keys, "keys must not be null");

    if (fromIndex < 0) {
      throw new IllegalArgumentException("fromIndex must be >= 0");
    }
    if (toIndex < fromIndex) {
      throw new IllegalArgumentException("toIndex must be >= fromIndex");
    }
    if (toIndex > keys.size()) {
      throw new IllegalArgumentException("toIndex must be <= keys.size()");
    }

    int lo = fromIndex;
    int hi = toIndex;

    while (lo < hi) {
      int mid = lo + ((hi - lo) >>> 1);
      if (keys.keyAt(mid) < key) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    return lo;
  }

  static int lowerBound(long[] keys, long key, int fromIndex, int toIndex) {
    Objects.requireNonNull(keys, "keys must not be null");
    return lowerBound(new InMemoryKeyAccessor(keys), key, fromIndex, toIndex);
  }

  private static int lowerBoundWithTiming(
      SortedKeyAccessor keys,
      long key,
      int fromIndex,
      int toIndex,
      LookupTiming timing) {
    Objects.requireNonNull(keys, "keys must not be null");
    Objects.requireNonNull(timing, "timing must not be null");

    if (fromIndex < 0) {
      throw new IllegalArgumentException("fromIndex must be >= 0");
    }
    if (toIndex < fromIndex) {
      throw new IllegalArgumentException("toIndex must be >= fromIndex");
    }
    if (toIndex > keys.size()) {
      throw new IllegalArgumentException("toIndex must be <= keys.size()");
    }

    int lo = fromIndex;
    int hi = toIndex;
    while (lo < hi) {
      int mid = lo + ((hi - lo) >>> 1);
      timing.recordKeyAtCall();
      if (keys.keyAt(mid) < key) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  static final class LookupTiming {
    private long keyAtCalls;
    private long boundWidthTotal;
    private long bucketHits;
    private long bucketMisses;
    private long emptyBuckets;

    void reset() {
      keyAtCalls = 0L;
      boundWidthTotal = 0L;
      bucketHits = 0L;
      bucketMisses = 0L;
      emptyBuckets = 0L;
    }

    void recordKeyAtCall() {
      keyAtCalls++;
    }

    void recordBoundWidth(int width) {
      if (width > 0) {
        boundWidthTotal += width;
      }
    }

    void recordBucket(boolean found, boolean empty) {
      if (empty) {
        emptyBuckets++;
      } else if (found) {
        bucketHits++;
      } else {
        bucketMisses++;
      }
    }

    long getKeyAtCalls() {
      return keyAtCalls;
    }

    long getBoundWidthTotal() {
      return boundWidthTotal;
    }

    long getBucketHits() {
      return bucketHits;
    }

    long getBucketMisses() {
      return bucketMisses;
    }

    long getEmptyBuckets() {
      return emptyBuckets;
    }
  }

  private static void validateSortedKeys(SortedKeyAccessor keys) {
    Objects.requireNonNull(keys, "keys must not be null");

    if (keys.size() == 0) {
      throw new IllegalArgumentException("sortedKeys must be non-empty");
    }

    long prev = keys.keyAt(0);
    if (prev < 0) {
      throw new IllegalArgumentException("sortedKeys must contain only non-negative keys");
    }

    for (int i = 1; i < keys.size(); i++) {
      long key = keys.keyAt(i);

      if (key < 0) {
        throw new IllegalArgumentException("sortedKeys must contain only non-negative keys");
      }
      if (key <= prev) {
        throw new IllegalArgumentException("sortedKeys must be strictly increasing");
      }

      prev = key;
    }
  }

  @Override
  public String toString() {
    return "RadixSplineLookup{"
        + "size=" + keyAccessor.size()
        + ", model=" + model
        + '}';
  }
}