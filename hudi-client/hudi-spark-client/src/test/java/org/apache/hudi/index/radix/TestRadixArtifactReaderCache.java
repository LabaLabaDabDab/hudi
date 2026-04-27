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
 * WITHOUT WARRANTIES OR ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.index.radix;

import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRadixArtifactReaderCache extends HoodieSparkClientTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initHoodieStorage();
    initMetaClient();
    RadixArtifactReaderCache.clear();
  }

  @AfterEach
  public void tearDown() throws Exception {
    RadixArtifactReaderCache.clear();
    cleanupResources();
  }

  @Test
  public void testIndexCloseEvictsReadersUnderTableRadixStaging() throws Exception {
    Path staging = Path.of(basePath, ".hoodie", ".radix_index_tmp");
    Files.createDirectories(staging);
    PartitionLookupDescriptor desc = writeOneEntryArtifactForTest(staging);

    RadixArtifactReaderCache.getOrOpen(desc, storageConf);
    assertEquals(1, RadixArtifactReaderCache.cacheSizeForTesting());

    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(makeConfig());
    index.close();

    assertEquals(0, RadixArtifactReaderCache.cacheSizeForTesting());
  }

  @Test
  public void testEvictForTableLeavesOtherTableEntries() throws Exception {
    Path stagingA = Path.of(basePath, ".hoodie", ".radix_index_tmp");
    Files.createDirectories(stagingA);
    PartitionLookupDescriptor descA = writeOneEntryArtifactForTest(stagingA);
    RadixArtifactReaderCache.getOrOpen(descA, storageConf);

    Path baseB = Files.createDirectories(tempDir.resolve("table-b"));
    Path stagingB = baseB.resolve(".hoodie").resolve(".radix_index_tmp");
    Files.createDirectories(stagingB);
    PartitionLookupDescriptor descB = writeOneEntryArtifactForTest(stagingB);
    RadixArtifactReaderCache.getOrOpen(descB, storageConf);

    assertEquals(2, RadixArtifactReaderCache.cacheSizeForTesting());

    RadixArtifactReaderCache.evictForTable(basePath);

    assertEquals(1, RadixArtifactReaderCache.cacheSizeForTesting());
    RadixArtifactReaderCache.evictForTable(baseB.toAbsolutePath().toString());
    assertEquals(0, RadixArtifactReaderCache.cacheSizeForTesting());
  }

  @Test
  public void testRollbackCommitEvictsOnlyReadersForRolledInstant() throws Exception {
    Path staging = Path.of(basePath, ".hoodie", ".radix_index_tmp");
    Files.createDirectories(staging);

    PartitionLookupDescriptor d1 = writeOneEntryArtifactForTest(staging, "p1", "001");
    PartitionLookupDescriptor d2 = writeOneEntryArtifactForTest(staging, "p2", "002");

    RadixArtifactReaderCache.getOrOpen(d1, storageConf);
    RadixArtifactReaderCache.getOrOpen(d2, storageConf);
    assertEquals(2, RadixArtifactReaderCache.cacheSizeForTesting());

    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(makeConfig());
    assertTrue(index.rollbackCommit("001"));

    assertEquals(1, RadixArtifactReaderCache.cacheSizeForTesting());
    RadixArtifactReaderCache.getOrOpen(d2, storageConf);
    assertEquals(1, RadixArtifactReaderCache.cacheSizeForTesting());
  }

  @Test
  public void testEntryAtIfEncodedKeyMismatchReturnsNullMatchReturnsFullEntry() throws Exception {
    Path staging = Path.of(basePath, ".hoodie", ".radix_index_tmp");
    Files.createDirectories(staging);
    PartitionLookupDescriptor desc = writeOneEntryArtifactForTest(staging);

    try (SimpleTempRadixArtifactReader reader =
        SimpleTempRadixArtifactReader.open(desc.getArtifactPath(), storageConf)) {

      assertNull(reader.entryAtIfEncodedKeyMatches(0, 11L));

      RadixLocationEntry expected =
          new RadixLocationEntry(10L, "k1", new HoodieRecordLocation("001", "f1"));
      assertEquals(expected, reader.entryAtIfEncodedKeyMatches(0, 10L));
    }
  }

  @Test
  public void testMaxEntriesPerPartitionFailsFast() throws Exception {
    Path staging = Path.of(basePath, ".hoodie", ".radix_index_tmp");
    Files.createDirectories(staging);
    InMemoryRadixEntrySorter sorter = new InMemoryRadixEntrySorter();
    for (long k = 1; k <= 3; k++) {
      sorter.add(
          new RadixLocationEntry(k, "k" + k, new HoodieRecordLocation("001", "f" + k)));
    }
    SortedRadixEntrySource src = sorter.finish();
    Path scratch = HoodieRadixSplineIndex.resolveLocalWriterScratchDir(new StoragePath(basePath));
    SimpleTempRadixArtifactWriter writer =
        new SimpleTempRadixArtifactWriter(new LocalRadixArtifactPublisher(staging), 2, scratch);
    try {
      HoodieIOException ex =
          assertThrows(
              HoodieIOException.class,
              () -> writer.write("part", "001", src, 2, 4, "fp", 1));
      assertTrue(ex.getMessage().contains("max_entries_per_partition"));
    } finally {
      src.close();
      sorter.close();
      writer.close();
    }
  }

  private HoodieWriteConfig makeConfig() {
    Properties props = new Properties();
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.RADIX_SPLINE)
            .build())
        .build();
  }

  static PartitionLookupDescriptor writeOneEntryArtifactForTest(Path stagingRoot)
      throws Exception {
    return writeOneEntryArtifactForTest(stagingRoot, "part", "001");
  }

  static PartitionLookupDescriptor writeOneEntryArtifactForTest(
      Path stagingRoot, String partitionPath, String baseInstant) throws Exception {
    InMemoryRadixEntrySorter sorter = new InMemoryRadixEntrySorter();
    sorter.add(
        new RadixLocationEntry(10L, "k1", new HoodieRecordLocation(baseInstant, "f1")));
    SortedRadixEntrySource src = sorter.finish();
    Path tableBase = stagingRoot.getParent().getParent();
    Path scratch =
        HoodieRadixSplineIndex.resolveLocalWriterScratchDir(
            new StoragePath(tableBase.toAbsolutePath().toString()));
    SimpleTempRadixArtifactWriter writer =
        new SimpleTempRadixArtifactWriter(new LocalRadixArtifactPublisher(stagingRoot), 0, scratch);
    try {
      return writer.write(partitionPath, baseInstant, src, 2, 4, "fp", 1);
    } finally {
      src.close();
      sorter.close();
      writer.close();
    }
  }
}
