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

import org.apache.hudi.io.SeekableDataInputStream;

import java.io.IOException;
import java.util.Objects;

/**
 * Reads encoded keys from a fixed byte offset inside a seekable stream (shared lock with other readers).
 */
final class SeekableStreamKeyAccessor implements SortedKeyAccessor {

  private static final long serialVersionUID = 1L;

  private final transient SeekableDataInputStream input;
  private final transient Object streamLock;
  private final long keysOffset;
  private final int size;

  SeekableStreamKeyAccessor(
      SeekableDataInputStream input,
      Object streamLock,
      long keysOffset,
      int size) {
    this.input = Objects.requireNonNull(input, "input must not be null");
    this.streamLock = Objects.requireNonNull(streamLock, "streamLock must not be null");
    this.keysOffset = keysOffset;
    this.size = size;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public long keyAt(int index) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException(
          "index=" + index + ", size=" + size);
    }

    try {
      synchronized (streamLock) {
        input.seek(keysOffset + ((long) index * Long.BYTES));
        return input.readLong();
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to read key at index=" + index, ioe);
    }
  }
}
