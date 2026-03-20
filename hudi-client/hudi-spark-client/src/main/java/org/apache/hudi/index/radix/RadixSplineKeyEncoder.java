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
 * Encodes record keys into a monotonic non-negative long domain suitable for Radix Spline.
 *
 * <p>Supported modes:
 * <ul>
 *   <li>NUMERIC_COLUMN - integer-like columns whose string form must be canonical decimal</li>
 *   <li>STRING_DECIMAL_COLUMN - string-like columns containing canonical decimal values</li>
 * </ul>
 *
 * <p>Canonical decimal means:
 * <ul>
 *   <li>"0" OR [1-9][0-9]*</li>
 *   <li>no '+' sign</li>
 *   <li>no leading zeros except "0"</li>
 *   <li>must fit into signed {@code long}</li>
 * </ul>
 */
final class RadixSplineKeyEncoder implements Serializable {

  private static final long serialVersionUID = 1L;

  enum Mode {
    NUMERIC_COLUMN,
    STRING_DECIMAL_COLUMN
  }

  private final Mode mode;

  RadixSplineKeyEncoder(Mode mode) {
    this.mode = Objects.requireNonNull(mode, "mode must not be null");
  }

  long encode(String recordKey) {
    switch (mode) {
      case NUMERIC_COLUMN:
      case STRING_DECIMAL_COLUMN:
        return parseCanonicalNonNegativeLong(recordKey);
      default:
        throw new IllegalArgumentException("Unsupported encoder mode: " + mode);
    }
  }

  boolean isSupported(String recordKey) {
    try {
      encode(recordKey);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private static long parseCanonicalNonNegativeLong(String value) {
    if (value == null) {
      throw new IllegalArgumentException("recordKey must not be null");
    }
    if (value.isEmpty()) {
      throw new IllegalArgumentException("recordKey must not be empty");
    }

    char first = value.charAt(0);
    if (first == '+') {
      throw new IllegalArgumentException("leading '+' is not supported: " + value);
    }
    if (first == '-') {
      throw new IllegalArgumentException("negative recordKey is not supported: " + value);
    }
    if (value.length() > 1 && first == '0') {
      throw new IllegalArgumentException("leading zeros are not supported: " + value);
    }

    long result = 0L;
    for (int i = 0; i < value.length(); i++) {
      char ch = value.charAt(i);
      if (ch < '0' || ch > '9') {
        throw new IllegalArgumentException(
            "non-numeric recordKey is not supported by RadixSplineKeyEncoder: " + value);
      }

      int digit = ch - '0';
      if (result > (Long.MAX_VALUE - digit) / 10L) {
        throw new IllegalArgumentException(
            "recordKey exceeds Long.MAX_VALUE and is not supported: " + value);
      }

      result = result * 10L + digit;
    }

    return result;
  }
}