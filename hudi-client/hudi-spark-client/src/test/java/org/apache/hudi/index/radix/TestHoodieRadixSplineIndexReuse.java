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

import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.HoodieTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieRadixSplineIndexReuse {

  @TempDir
  Path tempDir;

  private HoodieWriteConfig config;
  private HoodieTable hoodieTable;

  /** Partition -> base files returned by {@code getLatestBaseFilesBeforeOrOn}. */
  private final Map<String, List<HoodieBaseFile>> radixBaseFilesByPartition = new HashMap<>();

  @BeforeEach
  public void setUp() {
    config = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.toUri().toString())
        .forTable("radix_reuse_test")
        .withSchema("{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}")
        .withProps(Collections.singletonMap(
            KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id"))
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(org.apache.hudi.index.HoodieIndex.IndexType.RADIX_SPLINE)
            .build())
        .build();

    radixBaseFilesByPartition.clear();
    hoodieTable = mock(HoodieTable.class, RETURNS_DEEP_STUBS);

    HoodieInstant fakeLastInstant = mock(HoodieInstant.class);
    when(fakeLastInstant.requestedTime()).thenReturn("999");
    HoodieTimeline filteredTimeline = mock(HoodieTimeline.class);
    when(filteredTimeline.lastInstant()).thenReturn(Option.of(fakeLastInstant));
    HoodieTimeline commitsTimeline = mock(HoodieTimeline.class);
    when(commitsTimeline.filterCompletedInstants()).thenReturn(filteredTimeline);
    when(hoodieTable.getMetaClient().getCommitsTimeline()).thenReturn(commitsTimeline);
    when(hoodieTable.getBaseFileOnlyView().getLatestBaseFilesBeforeOrOn(anyString(), anyString()))
        .thenAnswer(invocation -> {
          String partition = invocation.getArgument(0);
          List<HoodieBaseFile> files =
              radixBaseFilesByPartition.getOrDefault(partition, Collections.emptyList());
          return files.stream();
        });

    HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(false);
    HoodieStorage storage =
        HoodieStorageUtils.getStorage(new StoragePath(tempDir.toUri().toString()), storageConf);
    org.mockito.Mockito.when(hoodieTable.getStorageConf()).thenReturn(storageConf);
    org.mockito.Mockito.when(hoodieTable.getStorage()).thenReturn(storage);
  }

  @Test
  public void testBuildPartitionLookupDescriptorReusesWhenPartitionStateIsUnchanged() {
    AtomicInteger writeCalls = new AtomicInteger();
    TestableIndex index = new TestableIndex(config, tempDir.resolve(".radix"), writeCalls);

    String partition = "p1";
    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "001", "/tmp/file1.parquet")
    ));

    PartitionLookupDescriptor first = index.buildPartitionLookupDescriptor(hoodieTable, partition);
    assertNotNull(first);
    assertEquals(1, writeCalls.get());

    PartitionLookupDescriptor second = index.buildPartitionLookupDescriptor(hoodieTable, partition);
    assertNotNull(second);

    assertEquals(1, writeCalls.get(), "second call should reuse existing artifact");
    assertEquals(first.getArtifactPath(), second.getArtifactPath());
    assertEquals(first.getBaseInstant(), second.getBaseInstant());
    assertEquals(first.getPartitionFingerprint(), second.getPartitionFingerprint());
    assertEquals(first.getFileCount(), second.getFileCount());
  }

  @Test
  public void testBuildPartitionLookupDescriptorRebuildsWhenPartitionStateChanges() {
    AtomicInteger writeCalls = new AtomicInteger();
    TestableIndex index = new TestableIndex(config, tempDir.resolve(".radix"), writeCalls);

    String partition = "p1";

    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "001", "/tmp/file1.parquet")
    ));

    PartitionLookupDescriptor first = index.buildPartitionLookupDescriptor(hoodieTable, partition);
    assertNotNull(first);
    assertEquals(1, writeCalls.get());

    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "002", "/tmp/file1_v2.parquet")
    ));

    PartitionLookupDescriptor second = index.buildPartitionLookupDescriptor(hoodieTable, partition);
    assertNotNull(second);

    assertEquals(2, writeCalls.get(), "changed partition state should trigger rebuild");
    assertNotEquals(first.getArtifactPath(), second.getArtifactPath());
    assertNotEquals(first.getPartitionFingerprint(), second.getPartitionFingerprint());
    assertEquals("002", second.getBaseInstant());
  }

  @Test
  public void testBuildPartitionLookupDescriptorRebuildsWhenArtifactIsMissing() throws Exception {
    AtomicInteger writeCalls = new AtomicInteger();
    TestableIndex index = new TestableIndex(config, tempDir.resolve(".radix"), writeCalls);

    String partition = "p1";
    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "001", "/tmp/file1.parquet")
    ));

    PartitionLookupDescriptor first = index.buildPartitionLookupDescriptor(hoodieTable, partition);
    assertNotNull(first);
    assertEquals(1, writeCalls.get());

    Files.deleteIfExists(Paths.get(URI.create(first.getArtifactPath())));

    PartitionLookupDescriptor second = index.buildPartitionLookupDescriptor(hoodieTable, partition);
    assertNotNull(second);

    assertEquals(2, writeCalls.get(), "missing artifact should force rebuild");
    assertNotEquals(first.getArtifactPath(), second.getArtifactPath());
  }

  @Test
  public void testBuildPartitionLookupDescriptorRebuildsWhenManifestIsBroken() throws Exception {
    AtomicInteger writeCalls = new AtomicInteger();
    Path root = tempDir.resolve(".radix");
    TestableIndex index = new TestableIndex(config, root, writeCalls);

    String partition = "p1";
    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "001", "/tmp/file1.parquet")
    ));

    PartitionLookupDescriptor first = index.buildPartitionLookupDescriptor(hoodieTable, partition);
    assertNotNull(first);
    assertEquals(1, writeCalls.get());

    Path manifest = root.resolve("latest").resolve(partitionFileName(partition) + ".properties");
    Properties props = new Properties();
    try (InputStream in = Files.newInputStream(manifest)) {
      props.load(in);
    }

    props.setProperty("fileCount", "not-a-number");

    try (OutputStream out = Files.newOutputStream(manifest)) {
      props.store(out, "broken manifest");
    }

    PartitionLookupDescriptor second = index.buildPartitionLookupDescriptor(hoodieTable, partition);
    assertNotNull(second);

    assertEquals(2, writeCalls.get(), "broken manifest should force rebuild");
    assertNotEquals(first.getArtifactPath(), second.getArtifactPath());
  }

  @Test
  public void testBuildPartitionLookupDescriptorIgnoresCleanupFailureAfterSuccessfulPublish() throws Exception {
    AtomicInteger writeCalls = new AtomicInteger();
    Path root = tempDir.resolve(".radix");

    CleanupFailingIndex index = new CleanupFailingIndex(config, root, writeCalls);

    String partition = "p1";
    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "001", "/tmp/file1.parquet")
    ));

    PartitionLookupDescriptor descriptor = index.buildPartitionLookupDescriptor(hoodieTable, partition);

    assertNotNull(descriptor, "descriptor should still be returned even if cleanup fails");
    assertEquals(1, writeCalls.get(), "artifact should still be written");

    Path artifactPath = Paths.get(URI.create(descriptor.getArtifactPath()));
    assertTrue(Files.exists(artifactPath), "artifact should remain on disk");

    Path manifestPath = root.resolve("latest").resolve(partitionFileName(partition) + ".properties");
    assertTrue(Files.exists(manifestPath), "latest manifest should be persisted");

    Properties props = new Properties();
    try (InputStream in = Files.newInputStream(manifestPath)) {
      props.load(in);
    }

    assertEquals("001", props.getProperty("baseInstant"));
    assertEquals(descriptor.getArtifactPath(), props.getProperty("artifactPath"));
    assertEquals(descriptor.getPartitionFingerprint(), props.getProperty("partitionFingerprint"));
    assertEquals(Integer.toString(descriptor.getFileCount()), props.getProperty("fileCount"));

    assertEquals(1, index.cleanupCalls.get(), "cleanup should have been attempted exactly once");
  }

  @Test
  public void testBuildPartitionLookupDescriptorCleansUpObsoleteArtifactsAfterRebuild() {
    AtomicInteger writeCalls = new AtomicInteger();
    Path root = tempDir.resolve(".radix");
    TestableIndex index = new TestableIndex(config, root, writeCalls);

    String partition = "p1";

    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "001", "/tmp/file1.parquet")
    ));

    PartitionLookupDescriptor first = index.buildPartitionLookupDescriptor(hoodieTable, partition);
    assertNotNull(first);

    Path oldInstantDir = root.resolve("instants").resolve("001").resolve(partitionFileName(partition));
    assertTrue(Files.exists(oldInstantDir));

    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "002", "/tmp/file1_v2.parquet")
    ));

    PartitionLookupDescriptor second = index.buildPartitionLookupDescriptor(hoodieTable, partition);
    assertNotNull(second);

    Path newInstantDir = root.resolve("instants").resolve("002").resolve(partitionFileName(partition));
    assertTrue(Files.exists(newInstantDir));
    assertFalse(Files.exists(oldInstantDir), "obsolete partition artifact dir should be removed");
  }

  @Test
  public void testBuildPartitionLookupDescriptorReturnsNullForEmptyPartition() {
    AtomicInteger writeCalls = new AtomicInteger();
    TestableIndex index = new TestableIndex(config, tempDir.resolve(".radix"), writeCalls);

    String partition = "p1";
    mockLatestBaseFiles(partition, Collections.emptyList());

    PartitionLookupDescriptor descriptor = index.buildPartitionLookupDescriptor(hoodieTable, partition);
    assertNull(descriptor);
    assertEquals(0, writeCalls.get());
  }

  private String partitionFileName(String partitionPath) {
    if (partitionPath == null || partitionPath.isEmpty()) {
      return "__root__";
    }
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(partitionPath.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testDifferentPartitionPathsDoNotCollideInManifestOrArtifactDirs() {
    AtomicInteger writeCalls = new AtomicInteger();
    Path root = tempDir.resolve(".radix");
    TestableIndex index = new TestableIndex(config, root, writeCalls);

    String partitionA = "a/b";
    String partitionB = "a_b";

    mockLatestBaseFiles(partitionA, Collections.singletonList(
        mockBaseFile("file1", "001", "/tmp/a-b.parquet")
    ));
    mockLatestBaseFiles(partitionB, Collections.singletonList(
        mockBaseFile("file2", "001", "/tmp/a_b.parquet")
    ));

    PartitionLookupDescriptor d1 = index.buildPartitionLookupDescriptor(hoodieTable, partitionA);
    PartitionLookupDescriptor d2 = index.buildPartitionLookupDescriptor(hoodieTable, partitionB);

    assertNotNull(d1);
    assertNotNull(d2);
    assertNotEquals(d1.getArtifactPath(), d2.getArtifactPath());

    Path manifestA = root.resolve("latest").resolve(partitionFileName(partitionA) + ".properties");
    Path manifestB = root.resolve("latest").resolve(partitionFileName(partitionB) + ".properties");

    assertTrue(Files.exists(manifestA));
    assertTrue(Files.exists(manifestB));
    assertNotEquals(manifestA, manifestB);

    Path dirA = root.resolve("instants").resolve("001").resolve(partitionFileName(partitionA));
    Path dirB = root.resolve("instants").resolve("001").resolve(partitionFileName(partitionB));

    assertTrue(Files.exists(dirA));
    assertTrue(Files.exists(dirB));
    assertNotEquals(dirA, dirB);
  }

  @Test
  public void testCleanupObsoleteArtifactsDoesNotTouchOtherPartitions() {
    AtomicInteger writeCalls = new AtomicInteger();
    Path root = tempDir.resolve(".radix");
    TestableIndex index = new TestableIndex(config, root, writeCalls);

    String p1 = "p1";
    String p2 = "p2";

    mockLatestBaseFiles(p1, Collections.singletonList(
        mockBaseFile("file1", "001", "/tmp/p1-v1.parquet")
    ));
    PartitionLookupDescriptor p1v1 = index.buildPartitionLookupDescriptor(hoodieTable, p1);
    assertNotNull(p1v1);

    mockLatestBaseFiles(p2, Collections.singletonList(
        mockBaseFile("file2", "001", "/tmp/p2-v1.parquet")
    ));
    PartitionLookupDescriptor p2v1 = index.buildPartitionLookupDescriptor(hoodieTable, p2);
    assertNotNull(p2v1);

    mockLatestBaseFiles(p1, Collections.singletonList(
        mockBaseFile("file1", "002", "/tmp/p1-v2.parquet")
    ));
    PartitionLookupDescriptor p1v2 = index.buildPartitionLookupDescriptor(hoodieTable, p1);
    assertNotNull(p1v2);

    Path p1OldDir = root.resolve("instants").resolve("001").resolve(partitionFileName(p1));
    Path p1NewDir = root.resolve("instants").resolve("002").resolve(partitionFileName(p1));
    Path p2Dir = root.resolve("instants").resolve("001").resolve(partitionFileName(p2));

    assertFalse(Files.exists(p1OldDir), "old dir for rebuilt partition should be removed");
    assertTrue(Files.exists(p1NewDir), "new dir for rebuilt partition should exist");
    assertTrue(Files.exists(p2Dir), "other partition must remain untouched");
  }

  private void mockLatestBaseFiles(String partition, List<HoodieBaseFile> files) {
    radixBaseFilesByPartition.put(partition, files);
    when(hoodieTable.getBaseFileOnlyView().getLatestBaseFiles(partition))
        .thenAnswer(invocation -> files.stream());
  }

  private HoodieBaseFile mockBaseFile(String fileId, String commitTime, String path) {
    HoodieBaseFile baseFile = mock(HoodieBaseFile.class);
    when(baseFile.getFileId()).thenReturn(fileId);
    when(baseFile.getCommitTime()).thenReturn(commitTime);
    when(baseFile.getPath()).thenReturn(path);
    return baseFile;
  }

  static final class EmptySortedRadixEntrySource implements SortedRadixEntrySource {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public RadixLocationEntry next() {
      throw new UnsupportedOperationException("no entries");
    }

    @Override
    public void close() {
      // no-op
    }
  }

  final class CleanupFailingIndex extends HoodieRadixSplineIndex {
    private final Path persistentRoot;
    private final AtomicInteger writeCalls;
    private final AtomicInteger cleanupCalls = new AtomicInteger();

    CleanupFailingIndex(HoodieWriteConfig writeConfig, Path persistentRoot, AtomicInteger writeCalls) {
      super(writeConfig);
      this.persistentRoot = persistentRoot;
      this.writeCalls = writeCalls;
    }

    @Override
    protected StoragePath resolvePersistentArtifactRoot(HoodieTable hoodieTable) {
      return new StoragePath(persistentRoot.toUri().toString());
    }

    @Override
    protected Path resolveLocalScratchForPartitionBuild(HoodieTable hoodieTable) throws IOException {
      Path scratch = persistentRoot.resolve(".writer_scratch");
      Files.createDirectories(scratch);
      return scratch;
    }

    @Override
    protected SpillableRadixEntrySorter createSorter(HoodieTable hoodieTable, Path localScratchDir) {
      return new NoOpSorter();
    }

    @Override
    protected TempRadixArtifactWriter createArtifactWriter(HoodieTable hoodieTable, Path localScratchDir) {
      return new RecordingWriter(persistentRoot, writeCalls);
    }

    @Override
    protected void streamPartitionEntries(
        HoodieTable hoodieTable,
        String partitionPath,
        RadixIOConsumer<RadixLocationEntry> consumer) {
      // no-op
    }

    @Override
    protected void cleanupObsoletePartitionArtifacts(
        HoodieTable hoodieTable,
        String partitionPath,
        String keepBaseInstant) {
      cleanupCalls.incrementAndGet();
      throw new RuntimeException("simulated cleanup failure");
    }
  }

  static final class NoOpSorter implements SpillableRadixEntrySorter {
    @Override
    public void add(RadixLocationEntry entry) {
      // no-op
    }

    @Override
    public SortedRadixEntrySource finish() {
      return new EmptySortedRadixEntrySource();
    }

    @Override
    public void close() {
      // no-op
    }
  }

  static final class RecordingWriter implements TempRadixArtifactWriter {
    private final Path artifactRoot;
    private final AtomicInteger writeCalls;

    RecordingWriter(Path artifactRoot, AtomicInteger writeCalls) {
      this.artifactRoot = artifactRoot;
      this.writeCalls = writeCalls;
    }

    @Override
    public PartitionLookupDescriptor write(
        String partitionPath,
        String baseInstant,
        SortedRadixEntrySource sortedEntries,
        int maxError,
        int radixBits,
        String partitionFingerprint,
        int fileCount) throws IOException {

      writeCalls.incrementAndGet();

      Path partitionDir = artifactRoot
          .resolve("instants")
          .resolve(baseInstant)
          .resolve(sanitizePartition(partitionPath));

      Files.createDirectories(partitionDir);
      Path artifact = Files.createTempFile(partitionDir, "radix-", ".bin");
      Files.write(artifact, new byte[] {1, 2, 3});

      return new PartitionLookupDescriptor(
          partitionPath,
          artifact.toUri().toString(),
          3L,
          10L,
          30L,
          baseInstant,
          partitionFingerprint,
          fileCount);
    }

    @Override
    public void close() {
      // no-op
    }

    private static String sanitizePartition(String partitionPath) {
      if (partitionPath == null || partitionPath.isEmpty()) {
        return "__root__";
      }
      return Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(partitionPath.getBytes(StandardCharsets.UTF_8));
    }
  }

  final class TestableIndex extends HoodieRadixSplineIndex {
    private final Path persistentRoot;
    private final AtomicInteger writeCalls;

    TestableIndex(HoodieWriteConfig writeConfig, Path persistentRoot, AtomicInteger writeCalls) {
      super(writeConfig);
      this.persistentRoot = persistentRoot;
      this.writeCalls = writeCalls;
    }

    @Override
    protected StoragePath resolvePersistentArtifactRoot(HoodieTable hoodieTable) {
      return new StoragePath(persistentRoot.toUri().toString());
    }

    @Override
    protected Path resolveLocalScratchForPartitionBuild(HoodieTable hoodieTable) throws IOException {
      Path scratch = persistentRoot.resolve(".writer_scratch");
      Files.createDirectories(scratch);
      return scratch;
    }

    @Override
    protected SpillableRadixEntrySorter createSorter(HoodieTable hoodieTable, Path localScratchDir) {
      return new NoOpSorter();
    }

    @Override
    protected TempRadixArtifactWriter createArtifactWriter(HoodieTable hoodieTable, Path localScratchDir) {
      return new RecordingWriter(persistentRoot, writeCalls);
    }

    @Override
    protected void streamPartitionEntries(
        HoodieTable hoodieTable,
        String partitionPath,
        RadixIOConsumer<RadixLocationEntry> consumer) {
      // no-op
    }
  }
}