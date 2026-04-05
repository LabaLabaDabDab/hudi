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

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.model.HoodieRecordLocation;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHoodieRadixSplineIndexTagLocationReuse {

  @TempDir
  Path tempDir;

  private HoodieWriteConfig config;
  private HoodieTable hoodieTable;
  private HoodieEngineContext context;

  private final Map<String, List<HoodieBaseFile>> radixBaseFilesByPartition = new HashMap<>();

  private String partitionFileName(String partitionPath) {
    if (partitionPath == null || partitionPath.isEmpty()) {
      return "__root__";
    }
    return java.util.Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(partitionPath.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  @BeforeEach
  public void setUp() {
    config = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.toUri().toString())
        .forTable("radix_tag_location_reuse_test")
        .withSchema("{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}")
        .withProps(Collections.singletonMap(
            KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id"))
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(org.apache.hudi.index.HoodieIndex.IndexType.RADIX_SPLINE)
            .build())
        .build();

    radixBaseFilesByPartition.clear();
    hoodieTable = mock(HoodieTable.class, RETURNS_DEEP_STUBS);
    context = mock(HoodieEngineContext.class);

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
  public void testTagLocationBuildsOnceAndReusesArtifactOnSecondCall() {
    AtomicInteger streamCalls = new AtomicInteger();
    TestableIndex index = new TestableIndex(config, tempDir.resolve(".radix"), streamCalls);

    String partition = "p1";
    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "001", "/tmp/file1.parquet")
    ));

    HoodieRecord<Object> existing1 = mockRecord("100", partition);
    HoodieRecord<Object> missing1 = mockRecord("999", partition);

    HoodieData<HoodieRecord<Object>> input1 =
        HoodieListData.eager(Arrays.asList(existing1, missing1));

    HoodieData<HoodieRecord<Object>> tagged1 =
        index.tagLocation(input1, context, hoodieTable);

    tagged1.collectAsList();

    assertEquals(1, streamCalls.get(), "first tagLocation should build artifact once");
    verify(existing1, times(1)).setCurrentLocation(
        new HoodieRecordLocation("001", "file-1"));
    verify(missing1, never()).setCurrentLocation(any());

    HoodieRecord<Object> existing2 = mockRecord("100", partition);
    HoodieRecord<Object> missing2 = mockRecord("999", partition);

    HoodieData<HoodieRecord<Object>> input2 =
        HoodieListData.eager(Arrays.asList(existing2, missing2));

    HoodieData<HoodieRecord<Object>> tagged2 =
        index.tagLocation(input2, context, hoodieTable);

    tagged2.collectAsList();

    assertEquals(1, streamCalls.get(), "second tagLocation should reuse artifact and skip rebuild");
    verify(existing2, times(1)).setCurrentLocation(
        new HoodieRecordLocation("001", "file-1"));
    verify(missing2, never()).setCurrentLocation(any());
  }

  @Test
  public void testTagLocationRebuildsArtifactWhenPartitionStateChanges() {
    AtomicInteger streamCalls = new AtomicInteger();
    RebuildableTagLocationIndex index =
        new RebuildableTagLocationIndex(config, tempDir.resolve(".radix"), streamCalls);

    String partition = "p1";

    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "001", "/tmp/file1.parquet")
    ));

    HoodieRecord<Object> firstRecord = mockRecord("100", partition);
    HoodieData<HoodieRecord<Object>> firstInput =
        HoodieListData.eager(Collections.singletonList(firstRecord));

    index.tagLocation(firstInput, context, hoodieTable).collectAsList();

    assertEquals(1, streamCalls.get(), "first tagLocation should build artifact");
    verify(firstRecord, times(1)).setCurrentLocation(
        new HoodieRecordLocation("001", "file-1"));

    index.setCurrentBuildVersion(2);

    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "002", "/tmp/file1_v2.parquet")
    ));

    HoodieRecord<Object> secondRecord = mockRecord("100", partition);
    HoodieData<HoodieRecord<Object>> secondInput =
        HoodieListData.eager(Collections.singletonList(secondRecord));

    index.tagLocation(secondInput, context, hoodieTable).collectAsList();

    assertEquals(2, streamCalls.get(), "second tagLocation should rebuild artifact");
    verify(secondRecord, times(1)).setCurrentLocation(
        new HoodieRecordLocation("002", "file-1-v2"));
  }

  @Test
  public void testTagLocationRebuildsWhenArtifactIsMissing() throws Exception {
    AtomicInteger streamCalls = new AtomicInteger();
    RebuildableTagLocationIndex index =
        new RebuildableTagLocationIndex(config, tempDir.resolve(".radix"), streamCalls);

    String partition = "p1";

    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "001", "/tmp/file1.parquet")
    ));

    HoodieRecord<Object> firstRecord = mockRecord("100", partition);
    HoodieData<HoodieRecord<Object>> firstInput =
        HoodieListData.eager(Collections.singletonList(firstRecord));

    index.tagLocation(firstInput, context, hoodieTable).collectAsList();

    assertEquals(1, streamCalls.get());

    PartitionLookupDescriptor descriptor =
        index.buildPartitionLookupDescriptor(hoodieTable, partition);
    assertNotNull(descriptor);

    Files.deleteIfExists(Paths.get(URI.create(descriptor.getArtifactPath())));

    HoodieRecord<Object> secondRecord = mockRecord("100", partition);
    HoodieData<HoodieRecord<Object>> secondInput =
        HoodieListData.eager(Collections.singletonList(secondRecord));

    index.tagLocation(secondInput, context, hoodieTable).collectAsList();

    assertEquals(2, streamCalls.get(), "missing artifact should force rebuild");
    verify(secondRecord, times(1)).setCurrentLocation(
        new HoodieRecordLocation("001", "file-1"));
  }

  @Test
  public void testTagLocationRebuildsWhenManifestIsBroken() throws Exception {
    AtomicInteger streamCalls = new AtomicInteger();
    Path root = tempDir.resolve(".radix");
    RebuildableTagLocationIndex index =
        new RebuildableTagLocationIndex(config, root, streamCalls);

    String partition = "p1";

    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "001", "/tmp/file1.parquet")
    ));

    HoodieRecord<Object> firstRecord = mockRecord("100", partition);
    HoodieData<HoodieRecord<Object>> firstInput =
        HoodieListData.eager(Collections.singletonList(firstRecord));

    index.tagLocation(firstInput, context, hoodieTable).collectAsList();

    assertEquals(1, streamCalls.get());

    String manifestFile =
        Base64.getUrlEncoder().withoutPadding().encodeToString(partition.getBytes(StandardCharsets.UTF_8))
            + ".properties";
    Path manifestPath = root.resolve("latest").resolve(manifestFile);
    Properties props = new Properties();
    try (InputStream in = Files.newInputStream(manifestPath)) {
      props.load(in);
    }

    props.setProperty("fileCount", "broken-value");

    try (OutputStream out = Files.newOutputStream(manifestPath)) {
      props.store(out, "broken manifest");
    }

    HoodieRecord<Object> secondRecord = mockRecord("100", partition);
    HoodieData<HoodieRecord<Object>> secondInput =
        HoodieListData.eager(Collections.singletonList(secondRecord));

    index.tagLocation(secondInput, context, hoodieTable).collectAsList();

    assertEquals(2, streamCalls.get(), "broken manifest should force rebuild");
    verify(secondRecord, times(1)).setCurrentLocation(
        new HoodieRecordLocation("001", "file-1"));
  }

  @Test
  public void testTagLocationRepeatedCallsReuseExistingArtifactWithoutRebuild() {
    AtomicInteger streamCalls = new AtomicInteger();
    Path root = tempDir.resolve(".radix");
    TestableIndex index = new TestableIndex(config, root, streamCalls);

    String partition = "p1";
    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "001", "/tmp/file1.parquet")
    ));

    HoodieRecord<Object> first = mockRecord("100", partition);
    HoodieRecord<Object> second = mockRecord("200", partition);

    HoodieData<HoodieRecord<Object>> input1 =
        HoodieListData.eager(java.util.Arrays.asList(first, second));

    index.tagLocation(input1, context, hoodieTable);
    assertEquals(1, streamCalls.get(), "first call should build artifact");

    HoodieRecord<Object> firstAgain = mockRecord("100", partition);
    HoodieRecord<Object> secondAgain = mockRecord("200", partition);

    HoodieData<HoodieRecord<Object>> input2 =
        HoodieListData.eager(java.util.Arrays.asList(firstAgain, secondAgain));

    index.tagLocation(input2, context, hoodieTable);
    assertEquals(1, streamCalls.get(), "second call should reuse artifact without rebuild");

    verify(firstAgain, times(1)).setCurrentLocation(new HoodieRecordLocation("001", "file-1"));
    verify(secondAgain, times(1)).setCurrentLocation(new HoodieRecordLocation("001", "file-2"));
  }

  @Test
  public void testTagLocationMissDoesNotMutateRecord() {
    AtomicInteger streamCalls = new AtomicInteger();
    Path root = tempDir.resolve(".radix");
    TestableIndex index = new TestableIndex(config, root, streamCalls);

    String partition = "p1";
    mockLatestBaseFiles(partition, Collections.singletonList(
        mockBaseFile("file1", "001", "/tmp/file1.parquet")
    ));

    HoodieRecord<Object> missing = mockRecord("999", partition);

    HoodieData<HoodieRecord<Object>> input =
        HoodieListData.eager(java.util.Collections.singletonList(missing));

    index.tagLocation(input, context, hoodieTable);

    verify(missing, never()).setCurrentLocation(any());
  }

  private HoodieRecord<Object> mockRecord(String recordKey, String partitionPath) {
    @SuppressWarnings("unchecked")
    HoodieRecord<Object> record = mock(HoodieRecord.class);
    when(record.getRecordKey()).thenReturn(recordKey);
    when(record.getPartitionPath()).thenReturn(partitionPath);
    return record;
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

  final class TestableIndex extends HoodieRadixSplineIndex {
    private final Path persistentRoot;
    private final AtomicInteger streamCalls;

    TestableIndex(HoodieWriteConfig writeConfig, Path persistentRoot, AtomicInteger streamCalls) {
      super(writeConfig);
      this.persistentRoot = persistentRoot;
      this.streamCalls = streamCalls;
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
      return new InMemoryRadixEntrySorter();
    }

    @Override
    protected TempRadixArtifactWriter createArtifactWriter(HoodieTable hoodieTable, Path localScratchDir) {
      return new SimpleTempRadixArtifactWriter(
          new LocalRadixArtifactPublisher(persistentRoot), 0, localScratchDir);
    }

    @Override
    protected void streamPartitionEntries(
        HoodieTable hoodieTable,
        String partitionPath,
        RadixIOConsumer<RadixLocationEntry> consumer) throws IOException {

      streamCalls.incrementAndGet();

      consumer.accept(new RadixLocationEntry(
          100L,
          "100",
          new HoodieRecordLocation("001", "file-1")));

      consumer.accept(new RadixLocationEntry(
          200L,
          "200",
          new HoodieRecordLocation("001", "file-2")));
    }
  }

  final class RebuildableTagLocationIndex extends HoodieRadixSplineIndex {
    private final Path persistentRoot;
    private final AtomicInteger streamCalls;
    private volatile int currentBuildVersion = 1;

    RebuildableTagLocationIndex(
        HoodieWriteConfig writeConfig,
        Path persistentRoot,
        AtomicInteger streamCalls) {
      super(writeConfig);
      this.persistentRoot = persistentRoot;
      this.streamCalls = streamCalls;
    }

    void setCurrentBuildVersion(int currentBuildVersion) {
      this.currentBuildVersion = currentBuildVersion;
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
      return new InMemoryRadixEntrySorter();
    }

    @Override
    protected TempRadixArtifactWriter createArtifactWriter(HoodieTable hoodieTable, Path localScratchDir) {
      return new SimpleTempRadixArtifactWriter(
          new LocalRadixArtifactPublisher(persistentRoot), 0, localScratchDir);
    }

    @Override
    protected void streamPartitionEntries(
        HoodieTable hoodieTable,
        String partitionPath,
        RadixIOConsumer<RadixLocationEntry> consumer) throws IOException {

      streamCalls.incrementAndGet();

      if (currentBuildVersion == 1) {
        consumer.accept(new RadixLocationEntry(
            100L,
            "100",
            new HoodieRecordLocation("001", "file-1")));
        consumer.accept(new RadixLocationEntry(
            200L,
            "200",
            new HoodieRecordLocation("001", "file-2")));
      } else {
        consumer.accept(new RadixLocationEntry(
            100L,
            "100",
            new HoodieRecordLocation("002", "file-1-v2")));
        consumer.accept(new RadixLocationEntry(
            200L,
            "200",
            new HoodieRecordLocation("002", "file-2-v2")));
      }
    }
  }
}