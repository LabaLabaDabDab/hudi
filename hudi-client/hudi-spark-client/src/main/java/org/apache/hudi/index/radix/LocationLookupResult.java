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
 * Result of exact lookup inside a bounded search window.
 */
final class LocationLookupResult implements Serializable {

  private static final long serialVersionUID = 1L;

  private final boolean found;
  private final Integer position;
  private final int insertPosition;
  private final SearchBound bound;

  private LocationLookupResult(boolean found, Integer position, int insertPosition, SearchBound bound) {
    if (insertPosition < 0) {
      throw new IllegalArgumentException("insertPosition must be >= 0");
    }
    this.found = found;
    this.position = position;
    this.insertPosition = insertPosition;
    this.bound = Objects.requireNonNull(bound, "bound must not be null");

    if (found && position == null) {
      throw new IllegalArgumentException("position must not be null when found=true");
    }
    if (!found && position != null) {
      throw new IllegalArgumentException("position must be null when found=false");
    }
  }

  static LocationLookupResult found(int position, SearchBound bound) {
    return new LocationLookupResult(true, position, position, bound);
  }

  static LocationLookupResult notFound(int insertPosition, SearchBound bound) {
    return new LocationLookupResult(false, null, insertPosition, bound);
  }

  boolean isFound() {
    return found;
  }

  int getPosition() {
    if (!found || position == null) {
      throw new IllegalStateException("position is available only when found=true");
    }
    return position;
  }

  int getInsertPosition() {
    return insertPosition;
  }

  SearchBound getBound() {
    return bound;
  }

  @Override
  public String toString() {
    return "LocationLookupResult{"
        + "found=" + found
        + ", position=" + position
        + ", insertPosition=" + insertPosition
        + ", bound=" + bound
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LocationLookupResult)) {
      return false;
    }
    LocationLookupResult that = (LocationLookupResult) o;
    return found == that.found
        && insertPosition == that.insertPosition
        && Objects.equals(position, that.position)
        && Objects.equals(bound, that.bound);
  }

  @Override
  public int hashCode() {
    return Objects.hash(found, position, insertPosition, bound);
  }
}