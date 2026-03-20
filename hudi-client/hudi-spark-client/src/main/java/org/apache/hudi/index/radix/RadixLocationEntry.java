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

import org.apache.hudi.common.model.HoodieRecordLocation;

import java.io.Serializable;
import java.util.Objects;

/**
 * Entry stored inside in-memory radix-backed location index.
 */
public final class RadixLocationEntry implements Serializable {

  private static final long serialVersionUID = 1L;

  private final long encodedKey;
  private final String recordKey;
  private final HoodieRecordLocation location;

  RadixLocationEntry(long encodedKey, String recordKey, HoodieRecordLocation location) {
    if (encodedKey < 0) {
      throw new IllegalArgumentException("encodedKey must be >= 0");
    }
    this.encodedKey = encodedKey;
    this.recordKey = Objects.requireNonNull(recordKey, "recordKey must not be null");
    this.location = Objects.requireNonNull(location, "location must not be null");
  }

  long getEncodedKey() {
    return encodedKey;
  }

  String getRecordKey() {
    return recordKey;
  }

  HoodieRecordLocation getLocation() {
    return location;
  }

  @Override
  public String toString() {
    return "RadixLocationEntry{"
        + "encodedKey=" + encodedKey
        + ", recordKey='" + recordKey + '\''
        + ", location=" + location
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RadixLocationEntry)) {
      return false;
    }
    RadixLocationEntry that = (RadixLocationEntry) o;
    return encodedKey == that.encodedKey
        && Objects.equals(recordKey, that.recordKey)
        && Objects.equals(location, that.location);
  }

  @Override
  public int hashCode() {
    return Objects.hash(encodedKey, recordKey, location);
  }
}