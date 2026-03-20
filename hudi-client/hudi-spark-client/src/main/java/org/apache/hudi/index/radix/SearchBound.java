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
 * Bounded exact-search window [lo, hi), where hi is exclusive.
 */
final class SearchBound implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int lo;
  private final int hi;

  SearchBound(int lo, int hi) {
    if (lo < 0) {
      throw new IllegalArgumentException("lo must be >= 0");
    }
    if (hi < lo) {
      throw new IllegalArgumentException("hi must be >= lo");
    }
    this.lo = lo;
    this.hi = hi;
  }

  int getLo() {
    return lo;
  }

  int getHi() {
    return hi;
  }

  int width() {
    return hi - lo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SearchBound)) {
      return false;
    }
    SearchBound that = (SearchBound) o;
    return lo == that.lo && hi == that.hi;
  }

  @Override
  public int hashCode() {
    return Objects.hash(lo, hi);
  }

  @Override
  public String toString() {
    return "SearchBound{"
        + "lo=" + lo
        + ", hi=" + hi
        + '}';
  }
}