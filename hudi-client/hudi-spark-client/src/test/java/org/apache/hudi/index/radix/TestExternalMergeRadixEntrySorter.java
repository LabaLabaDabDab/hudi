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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestExternalMergeRadixEntrySorter {

  @TempDir Path tempDir;

  @Test
  public void testSpillMergePreservesOrderAndTracksSpillBytes() throws IOException {
    int maxInMemory = 8;
    int totalEntries = 200;

    try (ExternalMergeRadixEntrySorter sorter =
             new ExternalMergeRadixEntrySorter(tempDir, maxInMemory)) {

      for (int i = 0; i < totalEntries; i++) {
        sorter.add(
            new RadixLocationEntry(
                (long) i,
                String.format("%010d", i),
                new HoodieRecordLocation("001", "f-" + (i % 3))));
      }

      assertTrue(
          sorter.getMergeSpillBytesWritten() > 0,
          "expected on-disk spill with maxInMemory=" + maxInMemory + " and " + totalEntries + " entries");
      assertTrue(
          sorter.getMergeSpillChunkCount() > 0,
          "expected at least one spill chunk file");

      try (SortedRadixEntrySource src = sorter.finish()) {
        long prev = -1L;
        int count = 0;
        while (src.hasNext()) {
          RadixLocationEntry e = src.next();
          assertTrue(
              e.getEncodedKey() > prev,
              "keys must be strictly increasing, got " + prev + " then " + e.getEncodedKey());
          prev = e.getEncodedKey();
          count++;
        }
        assertEquals(totalEntries, count);
      }
    }
  }

  @Test
  public void testFullyInMemoryNoSpill() throws IOException {
    Path dir = Files.createDirectories(tempDir.resolve("inmem"));
    try (ExternalMergeRadixEntrySorter sorter = new ExternalMergeRadixEntrySorter(dir, 10_000)) {
      for (int i = 0; i < 50; i++) {
        sorter.add(
            new RadixLocationEntry(
                (long) i,
                String.format("%010d", i),
                new HoodieRecordLocation("001", "f1")));
      }
      try (SortedRadixEntrySource src = sorter.finish()) {
        int count = 0;
        while (src.hasNext()) {
          src.next();
          count++;
        }
        assertEquals(50, count);
      }
      assertEquals(0L, sorter.getMergeSpillBytesWritten());
      assertEquals(0, sorter.getMergeSpillChunkCount());
    }
  }
}
