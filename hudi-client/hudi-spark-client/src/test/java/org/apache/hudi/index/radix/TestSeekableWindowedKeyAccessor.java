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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSeekableWindowedKeyAccessor {

  @Test
  public void lookupMatchesInMemoryAcrossWindows() throws IOException {
    int n = 5000;
    long[] keys = new long[n];
    for (int i = 0; i < n; i++) {
      keys[i] = (long) i * 3L + 1L;
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream(n * Long.BYTES);
    try (DataOutputStream out = new DataOutputStream(baos)) {
      for (long k : keys) {
        out.writeLong(k);
      }
    }
    byte[] raw = baos.toByteArray();
    long keysOffset = 0L;

    ByteArraySeekableDataInputStream in =
        new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(raw));
    Object streamLock = new Object();
    SeekableWindowedKeyAccessor windowed =
        new SeekableWindowedKeyAccessor(in, streamLock, keysOffset, n);

    int maxError = 4;
    int radixBits = 6;
    InMemoryKeyAccessor inMemory = new InMemoryKeyAccessor(keys);
    RadixSplineModel model = RadixSplineModel.build(inMemory, maxError, radixBits);
    RadixSplineLookup expected = RadixSplineLookup.fromModel(inMemory, model);
    RadixSplineLookup actual = RadixSplineLookup.fromModel(windowed, model);

    for (long probe : new long[] {-1L, 0L, 1L, 2L, 7L, keys[0], keys[n / 2], keys[n - 1], keys[n - 1] + 1L, Long.MAX_VALUE / 4}) {
      if (probe < 0) {
        continue;
      }
      assertEquals(
          expected.lookup(probe).isFound(),
          actual.lookup(probe).isFound(),
          "probe=" + probe);
      if (expected.lookup(probe).isFound()) {
        assertEquals(
            expected.lookup(probe).getPosition(),
            actual.lookup(probe).getPosition(),
            "probe=" + probe);
      }
    }

    for (int i = 0; i < n; i += 17) {
      long k = keys[i];
      assertEquals(expected.lookup(k).getPosition(), actual.lookup(k).getPosition());
    }
  }
}
