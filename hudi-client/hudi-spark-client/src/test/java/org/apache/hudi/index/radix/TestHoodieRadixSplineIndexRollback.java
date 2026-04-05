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

import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieRadixSplineIndexRollback extends HoodieSparkClientTestHarness {

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
  public void testRollbackCommitRemovesInstantStagingAndLatestManifestByBaseInstant() throws Exception {
    Path radix = Path.of(basePath, ".hoodie", ".radix_index_tmp");
    Path inst010 = radix.resolve("instants").resolve("010").resolve("cGFydA");
    Files.createDirectories(inst010);
    Path bin = inst010.resolve("artifact.bin");
    Files.write(bin, new byte[] {1});

    Path latest = radix.resolve("latest");
    Files.createDirectories(latest);
    Path manifest = latest.resolve("m1.properties");
    writeManifest(
        manifest,
        "part",
        "010",
        bin.toUri().toString(),
        "fp",
        1,
        1L,
        0L,
        1L);

    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(makeConfig());
    assertTrue(index.rollbackCommit("010"));

    assertFalse(Files.exists(radix.resolve("instants").resolve("010")));
    assertFalse(Files.exists(manifest));
  }

  @Test
  public void testRollbackCommitRemovesManifestWhenArtifactUnderRolledBackInstant() throws Exception {
    Path radix = Path.of(basePath, ".hoodie", ".radix_index_tmp");
    Path inst009 = radix.resolve("instants").resolve("009").resolve("cA");
    Files.createDirectories(inst009);
    Path bin = inst009.resolve("x.bin");
    Files.write(bin, new byte[] {2});

    Path latest = radix.resolve("latest");
    Files.createDirectories(latest);
    Path manifest = latest.resolve("m2.properties");
    writeManifest(
        manifest,
        "p",
        "011",
        bin.toUri().toString(),
        "fp2",
        1,
        1L,
        0L,
        1L);

    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(makeConfig());
    assertTrue(index.rollbackCommit("009"));

    assertFalse(Files.exists(radix.resolve("instants").resolve("009")));
    assertFalse(Files.exists(manifest));
  }

  @Test
  public void testRollbackCommitPreservesLatestManifestsForOtherInstants() throws Exception {
    Path radix = Path.of(basePath, ".hoodie", ".radix_index_tmp");
    Path latest = radix.resolve("latest");
    Files.createDirectories(latest);

    Path bin010 = radix.resolve("instants").resolve("010").resolve("p").resolve("a.bin");
    Files.createDirectories(bin010.getParent());
    Files.write(bin010, new byte[] {1});

    Path bin011 = radix.resolve("instants").resolve("011").resolve("p").resolve("b.bin");
    Files.createDirectories(bin011.getParent());
    Files.write(bin011, new byte[] {2});

    Path manifest010 = latest.resolve("m010.properties");
    writeManifest(
        manifest010,
        "part",
        "010",
        bin010.toUri().toString(),
        "fp10",
        1,
        1L,
        0L,
        1L);

    Path manifest011 = latest.resolve("m011.properties");
    writeManifest(
        manifest011,
        "part2",
        "011",
        bin011.toUri().toString(),
        "fp11",
        1,
        1L,
        0L,
        1L);

    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(makeConfig());
    assertTrue(index.rollbackCommit("010"));

    assertFalse(Files.exists(radix.resolve("instants").resolve("010")));
    assertFalse(Files.exists(manifest010));
    assertTrue(Files.exists(radix.resolve("instants").resolve("011")));
    assertTrue(Files.exists(manifest011));
  }

  @Test
  public void testRollbackCommitEvictsReaderCacheForRemovedManifest() throws Exception {
    Path staging = Path.of(basePath, ".hoodie", ".radix_index_tmp");
    Files.createDirectories(staging);
    PartitionLookupDescriptor desc = TestRadixArtifactReaderCache.writeOneEntryArtifactForTest(staging);
    RadixArtifactReaderCache.getOrOpen(desc, storageConf);
    assertEquals(1, RadixArtifactReaderCache.cacheSizeForTesting());

    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(makeConfig());
    assertTrue(index.rollbackCommit("001"));

    assertEquals(0, RadixArtifactReaderCache.cacheSizeForTesting());
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

  private static void writeManifest(
      Path manifestPath,
      String partitionPath,
      String baseInstant,
      String artifactPath,
      String partitionFingerprint,
      int fileCount,
      long entryCount,
      long minKey,
      long maxKey) throws Exception {

    Properties props = new Properties();
    props.setProperty("partitionPath", partitionPath);
    props.setProperty("baseInstant", baseInstant);
    props.setProperty("artifactPath", artifactPath);
    props.setProperty("partitionFingerprint", partitionFingerprint);
    props.setProperty("fileCount", Integer.toString(fileCount));
    props.setProperty("entryCount", Long.toString(entryCount));
    props.setProperty("minKey", Long.toString(minKey));
    props.setProperty("maxKey", Long.toString(maxKey));
    try (OutputStream out = Files.newOutputStream(manifestPath)) {
      props.store(out, "test manifest");
    }
  }
}
