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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * {@link SortedKeyAccessor} over the dense keys region inside a {@link SeekableDataInputStream},
 * using a sliding window of keys per {@code readFully} (same idea as {@link LongFileSortedKeyAccessor}).
 *
 * <p>Avoids one {@link SeekableDataInputStream#seek} + small read per {@link #keyAt} call, which
 * dominates {@link RadixSplineLookup#lowerBound} on remote storage.
 */
final class SeekableWindowedKeyAccessor implements SortedKeyAccessor {

  private static final long serialVersionUID = 1L;

  /** Max keys per window (32 KiB of longs). */
  private static final int DEFAULT_WINDOW_KEYS = 4096;

  /** If window reloads / keyAt samples exceeds this during calibration, grow the window. */
  private static final double ADAPTIVE_HIGH_RELOAD_RATIO = 0.08;

  /** If below this ratio, shrink the window after calibration. */
  private static final double ADAPTIVE_LOW_RELOAD_RATIO = 0.012;

  private final transient SeekableDataInputStream input;
  private final transient Object streamLock;
  private final long keysOffset;
  private final int size;

  private final RadixLookupWindowParams windowParams;
  private transient int effectiveWindowKeys;

  private transient byte[] windowBytes;
  private transient ByteBuffer decodeBuffer;
  private transient int windowStart = -1;
  private transient int windowCount;

  private transient int calibrationKeySamples;
  private transient long windowLoadsDuringCalibration;
  private transient boolean calibrationFinished;

  SeekableWindowedKeyAccessor(
      SeekableDataInputStream input,
      Object streamLock,
      long keysOffset,
      int size) {
    this(input, streamLock, keysOffset, size, RadixLookupWindowParams.fixed(DEFAULT_WINDOW_KEYS));
  }

  SeekableWindowedKeyAccessor(
      SeekableDataInputStream input,
      Object streamLock,
      long keysOffset,
      int size,
      int windowKeys) {
    this(input, streamLock, keysOffset, size, RadixLookupWindowParams.fixed(windowKeys));
  }

  SeekableWindowedKeyAccessor(
      SeekableDataInputStream input,
      Object streamLock,
      long keysOffset,
      int size,
      RadixLookupWindowParams windowParams) {
    this.input = Objects.requireNonNull(input, "input must not be null");
    this.streamLock = Objects.requireNonNull(streamLock, "streamLock must not be null");
    this.keysOffset = keysOffset;
    this.size = size;
    this.windowParams = Objects.requireNonNull(windowParams, "windowParams must not be null");
    this.effectiveWindowKeys = windowParams.getInitialWindowKeys();
    this.calibrationFinished = !windowParams.isAdaptive();
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public synchronized long keyAt(int index) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException("index=" + index + ", size=" + size);
    }
    try {
      if (windowStart >= 0
          && index >= windowStart
          && index < windowStart + windowCount) {
        long v = readWindowKey(index - windowStart);
        onKeyAtFinished();
        return v;
      }
      loadWindow(index);
      long v = readWindowKey(index - windowStart);
      onKeyAtFinished();
      return v;
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to read key at index=" + index, ioe);
    }
  }

  private void onKeyAtFinished() {
    if (!windowParams.isAdaptive() || calibrationFinished) {
      return;
    }
    calibrationKeySamples++;
    if (calibrationKeySamples >= windowParams.getCalibrationKeyAts()) {
      applyCalibration();
    }
  }

  private void loadWindow(int index) throws IOException {
    if (windowParams.isAdaptive() && !calibrationFinished) {
      windowLoadsDuringCalibration++;
    }
    ensureBuffers();
    int cap = windowBytes.length / Long.BYTES;
    windowStart = Math.max(0, Math.min(index, size - cap));
    windowCount = Math.min(cap, size - windowStart);
    int nbytes = windowCount * Long.BYTES;
    synchronized (streamLock) {
      input.seek(keysOffset + (long) windowStart * Long.BYTES);
      input.readFully(windowBytes, 0, nbytes);
    }
    decodeBuffer.clear();
    decodeBuffer.limit(nbytes);
  }

  /** Key {@code localIndex} within the current window (0 .. windowCount - 1). */
  private long readWindowKey(int localIndex) {
    return decodeBuffer.getLong(localIndex * Long.BYTES);
  }

  private void applyCalibration() {
    double ratio =
        windowLoadsDuringCalibration / (double) Math.max(1, calibrationKeySamples);
    int next = effectiveWindowKeys;
    if (ratio > ADAPTIVE_HIGH_RELOAD_RATIO) {
      next = Math.min(windowParams.getMaxWindowKeys(), effectiveWindowKeys * 2);
    } else if (ratio < ADAPTIVE_LOW_RELOAD_RATIO) {
      next = Math.max(windowParams.getMinWindowKeys(), effectiveWindowKeys / 2);
    }
    if (next != effectiveWindowKeys) {
      effectiveWindowKeys = next;
      invalidateWindowState();
    }
    calibrationFinished = true;
  }

  private void invalidateWindowState() {
    windowBytes = null;
    decodeBuffer = null;
    windowStart = -1;
    windowCount = 0;
  }

  private void ensureBuffers() {
    if (windowBytes != null) {
      return;
    }
    int cap = size == 0 ? 1 : Math.min(effectiveWindowKeys, size);
    windowBytes = new byte[cap * Long.BYTES];
    decodeBuffer = ByteBuffer.wrap(windowBytes).order(ByteOrder.BIG_ENDIAN);
  }
}
