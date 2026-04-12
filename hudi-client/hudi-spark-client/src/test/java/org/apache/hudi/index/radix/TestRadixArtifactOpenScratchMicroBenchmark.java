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

import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.SeekableDataInputStream;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Correctness for {@link RadixArtifactOpenScratch} and a stdout microbench vs per-call
 * {@code new byte[] + ByteBuffer.wrap} (same workload as artifact open array reads).
 */
public class TestRadixArtifactOpenScratchMicroBenchmark {

  @Test
  public void scratchMatchesAllocatingBaseline() throws IOException {
    Object lock = new Object();
    byte[] payload = buildPayload(new long[] {1L, -1L, Long.MAX_VALUE}, new int[] {0, -1, Integer.MAX_VALUE});
    SeekableDataInputStream in = streamFor(payload);

    RadixArtifactOpenScratch scratch = new RadixArtifactOpenScratch();
    long[] longsScratch = scratch.readLongArray(in, lock, 0L, 3);
    int[] intsScratch = scratch.readIntArray(in, lock, 3L * Long.BYTES, 3);

    in = streamFor(payload);
    long[] longsBase = readLongArrayAllocating(in, lock, 0L, 3);
    int[] intsBase = readIntArrayAllocating(in, lock, 3L * Long.BYTES, 3);

    assertArrayEquals(longsBase, longsScratch);
    assertArrayEquals(intsBase, intsScratch);
  }

  /**
   * Prints timings to surefire stdout: isolates decode + I/O pattern used at artifact open
   * (repeated reads of the same-sized regions). Expect modest win; dominated by readFully
   * for large arrays.
   */
  @Test
  public void printAllocVsScratchTimings() throws IOException {
    final int longCount = 50_000;
    final int intCount = 50_000;
    final int warmup = 30;
    final int timed = 80;

    byte[] payload = buildPayload(longCount, intCount);
    Object lock = new Object();
    long longOff = 0L;
    long intOff = (long) longCount * Long.BYTES;

    double sAlloc = measureRepeatedReadsAllocating(payload, lock, longOff, intOff, longCount, intCount, warmup, timed);
    double sScratch = measureRepeatedReadsScratch(payload, lock, longOff, intOff, longCount, intCount, warmup, timed);

    System.out.println(
        "[radix-open-scratch-microbench] allocating: "
            + String.format(Locale.ROOT, "%.4f", sAlloc)
            + " s; scratch: "
            + String.format(Locale.ROOT, "%.4f", sScratch)
            + " s; ratio scratch/alloc: "
            + String.format(Locale.ROOT, "%.3f", sScratch / sAlloc));
  }

  private static double measureRepeatedReadsAllocating(
      byte[] payload,
      Object lock,
      long longOff,
      long intOff,
      int longCount,
      int intCount,
      int warmup,
      int timed)
      throws IOException {
    for (int i = 0; i < warmup; i++) {
      SeekableDataInputStream in = streamFor(payload);
      readLongArrayAllocating(in, lock, longOff, longCount);
      readIntArrayAllocating(in, lock, intOff, intCount);
    }
    long t0 = System.nanoTime();
    for (int i = 0; i < timed; i++) {
      SeekableDataInputStream in = streamFor(payload);
      readLongArrayAllocating(in, lock, longOff, longCount);
      readIntArrayAllocating(in, lock, intOff, intCount);
    }
    return (System.nanoTime() - t0) / 1_000_000_000.0;
  }

  private static double measureRepeatedReadsScratch(
      byte[] payload,
      Object lock,
      long longOff,
      long intOff,
      int longCount,
      int intCount,
      int warmup,
      int timed)
      throws IOException {
    RadixArtifactOpenScratch scratch = new RadixArtifactOpenScratch();
    for (int i = 0; i < warmup; i++) {
      SeekableDataInputStream in = streamFor(payload);
      scratch.readLongArray(in, lock, longOff, longCount);
      scratch.readIntArray(in, lock, intOff, intCount);
    }
    long t0 = System.nanoTime();
    for (int i = 0; i < timed; i++) {
      SeekableDataInputStream in = streamFor(payload);
      scratch.readLongArray(in, lock, longOff, longCount);
      scratch.readIntArray(in, lock, intOff, intCount);
    }
    return (System.nanoTime() - t0) / 1_000_000_000.0;
  }

  private static ByteArraySeekableDataInputStream streamFor(byte[] payload) {
    ByteBuffer buf = ByteBuffer.wrap(payload);
    return new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(buf));
  }

  private static byte[] buildPayload(int longCount, int intCount) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(baos)) {
      for (int i = 0; i < longCount; i++) {
        out.writeLong((long) i * 1315423911L);
      }
      for (int i = 0; i < intCount; i++) {
        out.writeInt(i ^ (i << 13));
      }
    }
    return baos.toByteArray();
  }

  private static byte[] buildPayload(long[] longs, int[] ints) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(baos)) {
      for (long v : longs) {
        out.writeLong(v);
      }
      for (int v : ints) {
        out.writeInt(v);
      }
    }
    return baos.toByteArray();
  }

  /** Baseline: new backing array + wrap per call (pre-scratch behavior). */
  private static long[] readLongArrayAllocating(
      SeekableDataInputStream in, Object streamLock, long offset, int size) throws IOException {
    long[] result = new long[size];
    if (size == 0) {
      return result;
    }
    byte[] buf = new byte[size * Long.BYTES];
    synchronized (streamLock) {
      in.seek(offset);
      in.readFully(buf);
    }
    ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.BIG_ENDIAN);
    for (int i = 0; i < size; i++) {
      result[i] = bb.getLong();
    }
    return result;
  }

  private static int[] readIntArrayAllocating(
      SeekableDataInputStream in, Object streamLock, long offset, int size) throws IOException {
    int[] result = new int[size];
    if (size == 0) {
      return result;
    }
    byte[] buf = new byte[size * Integer.BYTES];
    synchronized (streamLock) {
      in.seek(offset);
      in.readFully(buf);
    }
    ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.BIG_ENDIAN);
    for (int i = 0; i < size; i++) {
      result[i] = bb.getInt();
    }
    return result;
  }
}
