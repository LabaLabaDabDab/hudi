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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Local microbench: fixed vs adaptive {@link SeekableWindowedKeyAccessor}; prints to stdout. */
public class TestRadixLookupWindowMicroBenchmark {

  @Test
  public void printFixedVsAdaptiveLookupTimings() throws IOException {
    final int n = Integer.getInteger("radix.microbench.n", 120_000);
    final int warmup = Integer.getInteger("radix.microbench.warmup", 25_000);
    final int timed = Integer.getInteger("radix.microbench.timed", 120_000);
    final int maxError = 16;
    final int radixBits = 8;
    final int calibrationKeyAts = 6_000;

    long[] keys = new long[n];
    for (int i = 0; i < n; i++) {
      keys[i] = (long) i * 2L + 1L;
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream(n * Long.BYTES);
    try (DataOutputStream out = new DataOutputStream(baos)) {
      for (long k : keys) {
        out.writeLong(k);
      }
    }
    final byte[] raw = baos.toByteArray();

    InMemoryKeyAccessor ref = new InMemoryKeyAccessor(keys);
    RadixSplineModel model = RadixSplineModel.build(ref, maxError, radixBits);
    RadixSplineLookup refLookup = RadixSplineLookup.fromModel(ref, model);

    SplittableRandom rng = new SplittableRandom(0xC0FFEE);
    long[] probes = new long[warmup + timed];
    for (int i = 0; i < probes.length; i++) {
      probes[i] = keys[rng.nextInt(n)];
    }

    RadixSplineLookup fixedLookup = windowedLookup(raw, n, RadixLookupWindowParams.fixed(4096), model);
    RadixSplineLookup adaptiveLookup =
        windowedLookup(
            raw,
            n,
            new RadixLookupWindowParams(4096, true, 1024, 8192, calibrationKeyAts),
            model);

    for (int i = 0; i < 100; i++) {
      long p = probes[i];
      assertEquals(refLookup.lookup(p).getInsertPosition(), fixedLookup.lookup(p).getInsertPosition(), "fixed i=" + i);
      assertEquals(refLookup.lookup(p).getInsertPosition(), adaptiveLookup.lookup(p).getInsertPosition(), "adapt i=" + i);
    }

    // Symmetric JIT warmup: alternate so neither variant only benefits from a hot JVM after the other.
    for (int round = 0; round < 4; round++) {
      runPhaseDiscard(fixedLookup, probes, warmup, timed);
      runPhaseDiscard(adaptiveLookup, probes, warmup, timed);
    }

    double sFixed = runPhaseMeasure("fixed_window_4096", fixedLookup, probes, warmup, timed);
    double sAdaptive = runPhaseMeasure("adaptive_init4096", adaptiveLookup, probes, warmup, timed);
    double sAdaptive2 = runPhaseMeasure("adaptive_init4096_repeat", adaptiveLookup, probes, warmup, timed);
    double sFixed2 = runPhaseMeasure("fixed_window_4096_repeat", fixedLookup, probes, warmup, timed);

    double avgFixed = (sFixed + sFixed2) / 2.0;
    double avgAdapt = (sAdaptive + sAdaptive2) / 2.0;
    System.out.println(
        "[radix-window-microbench] avg fixed (2 runs): "
            + String.format(Locale.ROOT, "%.4f", avgFixed)
            + " s; avg adaptive (2 runs): "
            + String.format(Locale.ROOT, "%.4f", avgAdapt)
            + " s; ratio adaptive/fixed: "
            + String.format(Locale.ROOT, "%.3f", avgAdapt / avgFixed));
  }

  private static RadixSplineLookup windowedLookup(
      byte[] raw, int n, RadixLookupWindowParams params, RadixSplineModel model) throws IOException {
    ByteBuffer buf = ByteBuffer.wrap(raw);
    ByteArraySeekableDataInputStream in =
        new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(buf));
    Object streamLock = new Object();
    SeekableWindowedKeyAccessor accessor =
        new SeekableWindowedKeyAccessor(in, streamLock, 0L, n, params);
    return RadixSplineLookup.fromModel(accessor, model);
  }

  private static void runPhaseDiscard(RadixSplineLookup lookup, long[] probes, int warmup, int timed) {
    for (int i = 0; i < warmup + timed; i++) {
      lookup.lookup(probes[i % probes.length]);
    }
  }

  /** @return elapsed seconds for the timed section only */
  private static double runPhaseMeasure(String label, RadixSplineLookup lookup, long[] probes, int warmup, int timed) {
    for (int i = 0; i < warmup; i++) {
      lookup.lookup(probes[i]);
    }
    long t0 = System.nanoTime();
    for (int i = warmup; i < warmup + timed; i++) {
      lookup.lookup(probes[i]);
    }
    long t1 = System.nanoTime();
    double sec = (t1 - t0) / 1_000_000_000.0;
    double rps = timed / sec;
    System.out.println(
        "[radix-window-microbench] "
            + label
            + ": "
            + String.format(Locale.ROOT, "%.4f", sec)
            + " s for "
            + timed
            + " lookups => "
            + String.format(Locale.ROOT, "%.0f", rps)
            + " lookups/s");
    return sec;
  }
}
