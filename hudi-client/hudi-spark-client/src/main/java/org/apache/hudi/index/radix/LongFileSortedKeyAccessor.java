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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;

/**
 * {@link SortedKeyAccessor} over a dense file of big-endian {@code long} keys ({@code size * 8} bytes).
 * Avoids holding all keys in a Java heap array during {@link RadixSplineModel#build}.
 *
 * <p>Uses a sliding window of keys read in one {@code readFully} to avoid a {@link RandomAccessFile#seek}
 * per {@link #keyAt} call (the spline builder revisits nearby indices).
 */
final class LongFileSortedKeyAccessor implements SortedKeyAccessor, AutoCloseable {

  private static final long serialVersionUID = 1L;

  /** Max keys to load per window (32 KiB of longs). */
  private static final int DEFAULT_WINDOW_KEYS = 4096;

  private final int size;
  private final transient RandomAccessFile raf;

  private transient long[] window;
  private transient byte[] windowBytes;
  private transient int windowStart = -1;
  private transient int windowCount;

  LongFileSortedKeyAccessor(Path keysFile, int size) throws IOException {
    if (size < 0) {
      throw new IllegalArgumentException("size must be >= 0");
    }
    this.size = size;
    this.raf = new RandomAccessFile(keysFile.toFile(), "r");
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public long keyAt(int index) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException("index=" + index + ", size=" + size);
    }
    if (windowStart >= 0
        && index >= windowStart
        && index < windowStart + windowCount) {
      return window[index - windowStart];
    }
    try {
      loadWindow(index);
      return window[index - windowStart];
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void ensureBuffers() {
    if (window != null) {
      return;
    }
    int cap = size == 0 ? 1 : Math.min(DEFAULT_WINDOW_KEYS, size);
    window = new long[cap];
    windowBytes = new byte[cap * Long.BYTES];
  }

  private void loadWindow(int index) throws IOException {
    ensureBuffers();
    int cap = window.length;
    windowStart = Math.max(0, Math.min(index, size - cap));
    windowCount = Math.min(cap, size - windowStart);
    raf.seek((long) windowStart * Long.BYTES);
    int nbytes = windowCount * Long.BYTES;
    raf.readFully(windowBytes, 0, nbytes);
    ByteBuffer bb = ByteBuffer.wrap(windowBytes, 0, nbytes).order(ByteOrder.BIG_ENDIAN);
    for (int i = 0; i < windowCount; i++) {
      window[i] = bb.getLong();
    }
  }

  @Override
  public void close() throws IOException {
    raf.close();
  }
}
