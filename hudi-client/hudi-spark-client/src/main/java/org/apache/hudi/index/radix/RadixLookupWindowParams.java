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
 * Tunables for {@link SeekableWindowedKeyAccessor} used during RADIX_SPLINE artifact reads.
 */
final class RadixLookupWindowParams implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int initialWindowKeys;
  private final boolean adaptive;
  private final int minWindowKeys;
  private final int maxWindowKeys;
  private final int calibrationKeyAts;

  RadixLookupWindowParams(
      int initialWindowKeys,
      boolean adaptive,
      int minWindowKeys,
      int maxWindowKeys,
      int calibrationKeyAts) {
    if (initialWindowKeys <= 0) {
      throw new IllegalArgumentException("initialWindowKeys must be > 0");
    }
    if (minWindowKeys <= 0 || maxWindowKeys <= 0) {
      throw new IllegalArgumentException("minWindowKeys and maxWindowKeys must be > 0");
    }
    if (minWindowKeys > maxWindowKeys) {
      throw new IllegalArgumentException(
          "minWindowKeys must be <= maxWindowKeys: min=" + minWindowKeys + ", max=" + maxWindowKeys);
    }
    if (initialWindowKeys < minWindowKeys || initialWindowKeys > maxWindowKeys) {
      throw new IllegalArgumentException(
          "initialWindowKeys must be within [min,max]: initial="
              + initialWindowKeys + ", min=" + minWindowKeys + ", max=" + maxWindowKeys);
    }
    if (adaptive && calibrationKeyAts <= 0) {
      throw new IllegalArgumentException("calibrationKeyAts must be > 0 when adaptive is true");
    }
    this.initialWindowKeys = initialWindowKeys;
    this.adaptive = adaptive;
    this.minWindowKeys = minWindowKeys;
    this.maxWindowKeys = maxWindowKeys;
    this.calibrationKeyAts = calibrationKeyAts;
  }

  static RadixLookupWindowParams fixed(int windowKeys) {
    return new RadixLookupWindowParams(windowKeys, false, windowKeys, windowKeys, 0);
  }

  int getInitialWindowKeys() {
    return initialWindowKeys;
  }

  boolean isAdaptive() {
    return adaptive;
  }

  int getMinWindowKeys() {
    return minWindowKeys;
  }

  int getMaxWindowKeys() {
    return maxWindowKeys;
  }

  int getCalibrationKeyAts() {
    return calibrationKeyAts;
  }

  /**
   * Stable discriminator for reader-cache keys: same artifact URI may map to multiple readers when
   * lookup window parameters differ.
   */
  String cacheKeySuffix() {
    if (!adaptive) {
      return "f:" + initialWindowKeys;
    }
    return "a:"
        + initialWindowKeys
        + ":"
        + minWindowKeys
        + ":"
        + maxWindowKeys
        + ":"
        + calibrationKeyAts;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RadixLookupWindowParams that = (RadixLookupWindowParams) o;
    return initialWindowKeys == that.initialWindowKeys
        && adaptive == that.adaptive
        && minWindowKeys == that.minWindowKeys
        && maxWindowKeys == that.maxWindowKeys
        && calibrationKeyAts == that.calibrationKeyAts;
  }

  @Override
  public int hashCode() {
    return Objects.hash(initialWindowKeys, adaptive, minWindowKeys, maxWindowKeys, calibrationKeyAts);
  }
}
