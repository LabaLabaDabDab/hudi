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

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.hudi.avro.model.HoodieRadixSplineIndexManifest;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.io.HoodieKeyLocationFetchHandle;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.HoodieTable;
import org.apache.hadoop.conf.Configuration;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.SparkSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Properties;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Radix spline index for record location tagging. Latest per-partition descriptors are stored under
 * {@literal <base>/.hoodie/.radix_index_tmp/} (writer spill files under {@code .writer_scratch/} for local file
 * tables, otherwise {@code java.io.tmpdir}); {@link #rollbackCommit(String)} removes staging for the rolled-back
 * instant and stale {@code latest/*.properties} manifests so open readers and on-disk pointers are not left dangling.
 * If cleanup is skipped or partially fails, the next {@link #tagLocation} still rebuilds safely: manifests are matched
 * against a fingerprint of current base files, and missing or inconsistent artifacts are ignored.
 */
public class HoodieRadixSplineIndex extends HoodieIndex<Object, Object> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieRadixSplineIndex.class);

  /**
   * Initial capacity for per-task tagLocation output list (Spark partition size varies; this reduces
   * ArrayList growth rounds for typical micro-batches without large overhead for tiny partitions).
   */
  private static final int TAG_LOCATION_OUTPUT_INITIAL_CAPACITY = 1024;

  private final int maxError;
  private final int radixBits;
  private final boolean profileTagLocation;
  private final RadixLookupWindowParams radixLookupWindowParams;

  private volatile RadixSplineKeyEncoder keyEncoder;
  private volatile String recordKeyField;
  private volatile String recordKeyFieldTypeDescription;

  public HoodieRadixSplineIndex(HoodieWriteConfig config) {
    super(config);
    Objects.requireNonNull(config, "config must not be null");
    this.maxError = config.getRadixSplineIndexMaxError();
    this.radixBits = config.getRadixSplineIndexRadixBits();
    this.profileTagLocation = config.getRadixSplineProfileTagLocation();
    int initialWin = config.getRadixSplineLookupWindowKeys();
    if (config.getRadixSplineLookupWindowAdaptive()) {
      this.radixLookupWindowParams =
          new RadixLookupWindowParams(
              initialWin,
              true,
              config.getRadixSplineLookupWindowAdaptiveMin(),
              config.getRadixSplineLookupWindowAdaptiveMax(),
              config.getRadixSplineLookupWindowAdaptiveCalibrationKeys());
    } else {
      this.radixLookupWindowParams = RadixLookupWindowParams.fixed(initialWin);
    }
  }

  private static int hashMapCapacityForExpectedSize(int expectedSize) {
    if (expectedSize <= 0) {
      return 16;
    }
    return Math.max(16, (int) (expectedSize / 0.75f) + 1);
  }

  private static final class PartitionBaseFileState {
    private final String latestBaseInstant;
    private final String partitionFingerprint;
    private final int fileCount;

    private PartitionBaseFileState(
        String latestBaseInstant,
        String partitionFingerprint,
        int fileCount) {
      this.latestBaseInstant = latestBaseInstant;
      this.partitionFingerprint = partitionFingerprint;
      this.fileCount = fileCount;
    }

    String getLatestBaseInstant() {
      return latestBaseInstant;
    }

    String getPartitionFingerprint() {
      return partitionFingerprint;
    }

    int getFileCount() {
      return fileCount;
    }
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records,
      HoodieEngineContext context,
      HoodieTable hoodieTable) {

    long started = System.nanoTime();
    LOG.info("RADIX tagLocation start");

    Objects.requireNonNull(records, "records must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(hoodieTable, "hoodieTable must not be null");

    ensureKeyEncoderInitialized();

    List<PartitionLookupDescriptor> descriptors = loadPartitionLookups(records, hoodieTable);
    if (descriptors.isEmpty()) {
      LOG.info("RADIX tagLocation no descriptors: elapsedMs={}",
          (System.nanoTime() - started) / 1_000_000L);
      return records;
    }

    LOG.info("RADIX tagLocation descriptors loaded: count={}, elapsedMs={}",
        descriptors.size(),
        (System.nanoTime() - started) / 1_000_000L);

    Map<String, PartitionLookupRuntime> runtimeByPartition = buildRuntimeMap(descriptors);
    final StorageConfiguration<?> storageConf = hoodieTable.getStorageConf();

    LOG.info("RADIX tagLocation runtime map built: partitions={}, elapsedMs={}",
        runtimeByPartition.size(),
        (System.nanoTime() - started) / 1_000_000L);

    long mapPlanStarted = System.nanoTime();
    LOG.info("RADIX tagLocation map plan start: partitions={}, elapsedMs={}",
        runtimeByPartition.size(),
        (System.nanoTime() - started) / 1_000_000L);

    final boolean profileTagLocation = this.profileTagLocation;
    final int readerMapCapacity = hashMapCapacityForExpectedSize(runtimeByPartition.size());
    HoodieData<HoodieRecord<R>> tagged = records.mapPartitions(recordIterator -> {
      List<HoodieRecord<R>> output = new ArrayList<>(TAG_LOCATION_OUTPUT_INITIAL_CAPACITY);

      TaskContext tc = TaskContext.get();
      final int sparkPartitionId = tc != null ? tc.partitionId() : -1;
      final long taskAttemptId = tc != null ? tc.taskAttemptId() : -1L;

      long recordsTotal = 0L;
      long recordsNoRuntime = 0L;
      long recordsIndexed = 0L;
      long encodeNs = 0L;
      long readerGetNs = 0L;
      long readerGetCalls = 0L;
      long lookupNs = 0L;
      long lookupCalls = 0L;
      long lookupKeyAtCalls = 0L;
      long lookupBoundWidthTotal = 0L;
      long lookupBucketHits = 0L;
      long lookupBucketMisses = 0L;
      long lookupEmptyBuckets = 0L;
      long entryReadNs = 0L;
      long entryReadCalls = 0L;
      long entryOffsetLookupNs = 0L;
      long entryPayloadReadNs = 0L;
      long entryPayloadReadCalls = 0L;
      long notFound = 0L;
      long badPosition = 0L;
      long keyMismatch = 0L;
      long taggedOk = 0L;

      // One reader per Hudi partition per Spark task — avoids repeated RadixArtifactReaderCache lookups
      // and synchronized paths on every row (global cache still opens underlying streams once).
      Map<String, TempRadixArtifactReader> readerByPartitionPath = new HashMap<>(readerMapCapacity);
      final RadixSplineLookup.LookupTiming lookupTiming =
          profileTagLocation ? new RadixSplineLookup.LookupTiming() : null;

      try {
        while (recordIterator.hasNext()) {
          HoodieRecord<R> record = recordIterator.next();
          recordsTotal++;
          String partitionPath = record.getPartitionPath();
          PartitionLookupRuntime runtime = runtimeByPartition.get(partitionPath);
          if (runtime == null) {
            recordsNoRuntime++;
            output.add(record);
            continue;
          }

          recordsIndexed++;
          String recordKey = record.getRecordKey();
          final long encodedKey;
          if (profileTagLocation) {
            long t0 = System.nanoTime();
            encodedKey = encodeRecordKeyOrThrow(
                recordKey,
                partitionPath,
                "tagLocation");
            encodeNs += System.nanoTime() - t0;
          } else {
            encodedKey = encodeRecordKeyOrThrow(
                recordKey,
                partitionPath,
                "tagLocation");
          }

          TempRadixArtifactReader reader = readerByPartitionPath.get(partitionPath);
          if (reader == null) {
            if (profileTagLocation) {
              long t0 = System.nanoTime();
              reader = runtime.reader(storageConf);
              readerGetNs += System.nanoTime() - t0;
              readerGetCalls++;
            } else {
              reader = runtime.reader(storageConf);
            }
            readerByPartitionPath.put(partitionPath, reader);
          }

          LocationLookupResult result;
          if (profileTagLocation) {
            long t0 = System.nanoTime();
            lookupTiming.reset();
            result = reader.getLookup().lookupWithTiming(encodedKey, lookupTiming);
            lookupNs += System.nanoTime() - t0;
            lookupCalls++;
            lookupKeyAtCalls += lookupTiming.getKeyAtCalls();
            lookupBoundWidthTotal += lookupTiming.getBoundWidthTotal();
            lookupBucketHits += lookupTiming.getBucketHits();
            lookupBucketMisses += lookupTiming.getBucketMisses();
            lookupEmptyBuckets += lookupTiming.getEmptyBuckets();
          } else {
            result = reader.getLookup().lookup(encodedKey);
          }
          if (!result.isFound()) {
            notFound++;
            output.add(record);
            continue;
          }

          int position = result.getPosition();
          if (position < 0 || position >= reader.size()) {
            badPosition++;
            output.add(record);
            continue;
          }

          RadixLocationEntry candidate;
          try {
            if (profileTagLocation) {
              long t0 = System.nanoTime();
              if (reader instanceof SimpleTempRadixArtifactReader) {
                SimpleTempRadixArtifactReader.EntryAtTiming timing =
                    new SimpleTempRadixArtifactReader.EntryAtTiming();
                candidate = ((SimpleTempRadixArtifactReader) reader).entryAtWithTiming(position, timing);
                entryOffsetLookupNs += timing.getOffsetLookupNs();
                entryPayloadReadNs += timing.getPayloadReadNs();
                entryPayloadReadCalls += timing.getPayloadReadCalls();
              } else {
                candidate = reader.entryAt(position);
              }
              entryReadNs += System.nanoTime() - t0;
              entryReadCalls++;
            } else {
              candidate = reader.entryAt(position);
            }
          } catch (IOException ioe) {
            throw new RuntimeException(
                "Failed to read radix artifact entry for partition="
                    + runtime.descriptor().getPartitionPath()
                    + ", position=" + position,
                ioe);
          }

          if (!recordKey.equals(candidate.getRecordKey())) {
            keyMismatch++;
            output.add(record);
            continue;
          }

          record.unseal();
          record.setCurrentLocation(candidate.getLocation());
          record.seal();
          taggedOk++;
          output.add(record);
        }

        if (profileTagLocation && recordsTotal > 0) {
          LOG.info(
              "RADIX tagLocation task profile: sparkPartitionId={}, taskAttemptId={}, recordsTotal={}, "
                  + "noRuntime={}, indexed={}, taggedOk={}, notFound={}, badPosition={}, keyMismatch={}, "
                  + "encodeMs={}, readerGetMs={} (calls={}), lookupMs={} (calls={}), "
                  + "lookupKeyAtCalls={}, lookupAvgKeyAtPerLookup={}, "
                  + "lookupAvgBoundWidth={}, lookupBucketHitRatio={}, lookupBucketMissRatio={}, lookupEmptyBucketRatio={}, "
                  + "entryReadMs={} (calls={}), entryOffsetLookupMs={}, entryPayloadReadMs={} (calls={})",
              sparkPartitionId,
              taskAttemptId,
              recordsTotal,
              recordsNoRuntime,
              recordsIndexed,
              taggedOk,
              notFound,
              badPosition,
              keyMismatch,
              encodeNs / 1_000_000L,
              readerGetNs / 1_000_000L,
              readerGetCalls,
              lookupNs / 1_000_000L,
              lookupCalls,
              lookupKeyAtCalls,
              lookupCalls > 0 ? ((double) lookupKeyAtCalls) / lookupCalls : 0.0d,
              lookupCalls > 0 ? ((double) lookupBoundWidthTotal) / lookupCalls : 0.0d,
              lookupCalls > 0 ? ((double) lookupBucketHits) / lookupCalls : 0.0d,
              lookupCalls > 0 ? ((double) lookupBucketMisses) / lookupCalls : 0.0d,
              lookupCalls > 0 ? ((double) lookupEmptyBuckets) / lookupCalls : 0.0d,
              entryReadNs / 1_000_000L,
              entryReadCalls,
              entryOffsetLookupNs / 1_000_000L,
              entryPayloadReadNs / 1_000_000L,
              entryPayloadReadCalls);
        }

        return output.iterator();
      } finally {
        cleanupPartitionLookupRuntimes(runtimeByPartition);
      }
    }, true);

    LOG.info("RADIX tagLocation map plan created: partitions={}, planBuildMs={}, totalDriverMs={}",
        runtimeByPartition.size(),
        (System.nanoTime() - mapPlanStarted) / 1_000_000L,
        (System.nanoTime() - started) / 1_000_000L);

    return tagged;
  }

  protected <R> List<PartitionLookupDescriptor> loadPartitionLookups(
      HoodieData<HoodieRecord<R>> records,
      HoodieTable hoodieTable) {

    ensureKeyEncoderInitialized();

    long started = System.nanoTime();

    Set<String> touchedPartitions = new LinkedHashSet<>(records
        .map(HoodieRecord::getPartitionPath)
        .distinct()
        .collectAsList());

    if (touchedPartitions.isEmpty()) {
      LOG.info("RADIX loadPartitionLookups: no touched partitions");
      return Collections.emptyList();
    }

    int parallelism = Math.min(getPartitionBuildParallelism(), touchedPartitions.size());
    LOG.info(
        "RADIX loadPartitionLookups start: touchedPartitions={}, parallelism={}",
        touchedPartitions.size(),
        parallelism);

    ExecutorService executor = createPartitionBuildExecutor(parallelism);
    try {
      List<CompletableFuture<PartitionLookupDescriptor>> futures = new ArrayList<>(touchedPartitions.size());

      for (String partitionPath : touchedPartitions) {
        CompletableFuture<PartitionLookupDescriptor> future =
            CompletableFuture.supplyAsync(() -> {
              try {
                return buildPartitionLookupDescriptor(hoodieTable, partitionPath);
              } catch (Throwable t) {
                throw new CompletionException(
                    "Failed to build RADIX descriptor for partition=" + partitionPath, t);
              }
            }, executor);

        futures.add(future);
      }

      List<PartitionLookupDescriptor> result = new ArrayList<>(touchedPartitions.size());
      for (CompletableFuture<PartitionLookupDescriptor> future : futures) {
        PartitionLookupDescriptor descriptor;
        try {
          descriptor = future.join();
        } catch (CompletionException ce) {
          Throwable cause = ce.getCause() != null ? ce.getCause() : ce;
          if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
          }
          throw new RuntimeException("Failed to load RADIX partition lookup descriptors", cause);
        }

        if (descriptor != null) {
          result.add(descriptor);
        }
      }

      long elapsedMs = (System.nanoTime() - started) / 1_000_000L;
      LOG.info(
          "RADIX loadPartitionLookups complete: touchedPartitions={}, loadedDescriptors={}, parallelism={}, elapsedMs={}",
          touchedPartitions.size(),
          result.size(),
          parallelism,
          elapsedMs);

      return result;
    } finally {
      executor.shutdown();
    }
  }

  protected StoragePath resolvePersistentArtifactRoot(HoodieTable hoodieTable) {
    StoragePath base = hoodieTable.getMetaClient().getBasePath();
    return new StoragePath(base, ".hoodie/.radix_index_tmp");
  }

  /**
   * Local directory for {@link SimpleTempRadixArtifactWriter} spill files (keys/offsets/entries).
   * For non-{@code file} schemes (e.g. cloud storage) returns {@code java.io.tmpdir} because spill files must live
   * on the local filesystem of the writing process.
   */
  static Path resolveLocalWriterScratchDir(StoragePath tableBasePath) throws IOException {
    StoragePath scratch =
        new StoragePath(new StoragePath(tableBasePath, ".hoodie/.radix_index_tmp"), ".writer_scratch");
    URI uri = scratch.toUri();
    String scheme = uri.getScheme();
    if (scheme != null && !scheme.equalsIgnoreCase("file")) {
      return Paths.get(System.getProperty("java.io.tmpdir"));
    }
    // Table base paths often use a URI without an explicit scheme; Paths.get(URI) then fails
    // ("Missing scheme"). Treat those as the default local filesystem path component.
    Path p =
        scheme == null || scheme.isEmpty()
            ? Paths.get(uri.getPath())
            : Paths.get(uri);
    Files.createDirectories(p);
    return p.toAbsolutePath().normalize();
  }

  /**
   * Local scratch for merge spill and radix writer temp files during partition rebuild. Override in tests when
   * {@link #resolveLocalWriterScratchDir} cannot derive a path from {@code metaClient.getBasePath()}.
   */
  protected Path resolveLocalScratchForPartitionBuild(HoodieTable hoodieTable) throws IOException {
    return resolveLocalWriterScratchDir(hoodieTable.getMetaClient().getBasePath());
  }

  private StoragePath resolveLatestManifestDir(HoodieTable hoodieTable) {
    return new StoragePath(resolvePersistentArtifactRoot(hoodieTable), "latest");
  }

  private StoragePath resolveLatestManifestPath(HoodieTable hoodieTable, String partitionPath) {
    return new StoragePath(
        resolveLatestManifestDir(hoodieTable),
        sanitizePartition(partitionPath) + ".properties");
  }

  private String sanitizePartition(String partitionPath) {
    if (partitionPath == null || partitionPath.isEmpty()) {
      return "__root__";
    }
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(partitionPath.getBytes(StandardCharsets.UTF_8));
  }

  private static String sha256Hex(String value) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 is not available", e);
    }
  }

  private PartitionBaseFileState resolvePartitionBaseFileState(
      HoodieTable hoodieTable,
      String partitionPath) {
    List<HoodieBaseFile> baseFiles = getReusableBaseFiles(hoodieTable, partitionPath);

    if (baseFiles.isEmpty()) {
      return null;
    }

    baseFiles.sort((l, r) -> {
      String lk = l.getFileId() + "|" + l.getCommitTime() + "|" + l.getPath();
      String rk = r.getFileId() + "|" + r.getCommitTime() + "|" + r.getPath();
      return lk.compareTo(rk);
    });

    String maxInstant = null;
    StringBuilder sb = new StringBuilder();
    for (HoodieBaseFile baseFile : baseFiles) {
      String commitTime = baseFile.getCommitTime();
      if (maxInstant == null || commitTime.compareTo(maxInstant) > 0) {
        maxInstant = commitTime;
      }

      sb.append(baseFile.getFileId()).append('|')
          .append(commitTime).append('|')
          .append(baseFile.getPath()).append('\n');
    }

    String fingerprint = sha256Hex(sb.toString());

    LOG.info(
        "RADIX resolved latest base files: partition={}, fileCount={}, latestBaseInstant={}, fingerprint={}",
        partitionPath,
        baseFiles.size(),
        maxInstant,
        fingerprint);

    return new PartitionBaseFileState(maxInstant, fingerprint, baseFiles.size());
  }

  protected int getPartitionBuildParallelism() {
    int cpus = Runtime.getRuntime().availableProcessors();
    return Math.max(1, Math.min(8, cpus));
  }

  protected ExecutorService createPartitionBuildExecutor(int parallelism) {
    ThreadFactory factory = runnable -> {
      Thread t = new Thread(runnable);
      t.setDaemon(true);
      t.setName("radix-partition-build-" + t.getId());
      return t;
    };
    return Executors.newFixedThreadPool(parallelism, factory);
  }

  private static String shortFingerprint(String fingerprint) {
    if (fingerprint == null || fingerprint.length() <= 12) {
      return fingerprint;
    }
    return fingerprint.substring(0, 12);
  }

  private PartitionLookupDescriptor buildDescriptorFromManifestFields(
      String partitionPath,
      HoodieRadixSplineIndexManifest m,
      PartitionBaseFileState expectedState,
      HoodieStorage storage) {
    if (!partitionPath.equals(m.getPartitionPath())) {
      LOG.info(
          "RADIX manifest partitionPath mismatch: expected={}, manifest={}",
          partitionPath,
          m.getPartitionPath());
      return null;
    }
    if (!expectedState.getLatestBaseInstant().equals(m.getBaseInstant())) {
      LOG.info(
          "RADIX manifest baseInstant mismatch: partition={}, expected={}, actual={}",
          partitionPath,
          expectedState.getLatestBaseInstant(),
          m.getBaseInstant());
      return null;
    }
    if (!expectedState.getPartitionFingerprint().equals(m.getPartitionFingerprint())) {
      LOG.info(
          "RADIX manifest fingerprint mismatch: partition={}, expected={}, actual={}",
          partitionPath,
          shortFingerprint(expectedState.getPartitionFingerprint()),
          shortFingerprint(m.getPartitionFingerprint()));
      return null;
    }
    if (m.getFileCount() != expectedState.getFileCount()) {
      LOG.info(
          "RADIX manifest fileCount mismatch: partition={}, expected={}, actual={}",
          partitionPath,
          expectedState.getFileCount(),
          m.getFileCount());
      return null;
    }
    StoragePath artifact;
    try {
      artifact = new StoragePath(m.getArtifactPath());
    } catch (Exception e) {
      LOG.info(
          "RADIX artifact path invalid: partition={}, artifactPath={}",
          partitionPath,
          m.getArtifactPath(),
          e);
      return null;
    }
    try {
      if (!storage.exists(artifact)) {
        LOG.info(
            "RADIX artifact missing: partition={}, artifactPath={}",
            partitionPath,
            m.getArtifactPath());
        return null;
      }
    } catch (IOException e) {
      LOG.info(
          "RADIX artifact existence check failed: partition={}, artifactPath={}",
          partitionPath,
          m.getArtifactPath(),
          e);
      return null;
    }
    return new PartitionLookupDescriptor(
        partitionPath,
        m.getArtifactPath(),
        m.getEntryCount(),
        m.getMinKey(),
        m.getMaxKey(),
        m.getBaseInstant(),
        m.getPartitionFingerprint(),
        m.getFileCount());
  }

  private PartitionLookupDescriptor tryLoadLatestDescriptor(
      HoodieTable hoodieTable,
      String partitionPath,
      PartitionBaseFileState expectedState) {

    HoodieStorage storage = hoodieTable.getStorage();
    if (hoodieTable.getMetaClient().getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RADIX_SPLINE_INDEX)) {
      Option<HoodieRadixSplineIndexManifest> mdtManifest =
          hoodieTable.getMetadataTable().getRadixSplineIndexManifest(sanitizePartition(partitionPath));
      if (mdtManifest.isPresent()) {
        PartitionLookupDescriptor fromMdt =
            buildDescriptorFromManifestFields(
                partitionPath, mdtManifest.get(), expectedState, storage);
        if (fromMdt != null) {
          LOG.info("RADIX manifest loaded from metadata table: partition={}", partitionPath);
          return fromMdt;
        }
      }
    }

    StoragePath manifestPath = resolveLatestManifestPath(hoodieTable, partitionPath);
    try {
      if (!storage.exists(manifestPath)) {
        LOG.info("RADIX manifest missing: partition={}, manifest={}", partitionPath, manifestPath);
        return null;
      }
    } catch (IOException e) {
      LOG.info("RADIX manifest existence check failed: partition={}, manifest={}", partitionPath, manifestPath, e);
      return null;
    }

    Properties props = new Properties();
    try (InputStream in = new BufferedInputStream(storage.open(manifestPath))) {
      props.load(in);
    } catch (IOException e) {
      LOG.info("RADIX manifest unreadable: partition={}, manifest={}", partitionPath, manifestPath, e);
      return null;
    }

    String baseInstant = props.getProperty("baseInstant");
    String artifactPath = props.getProperty("artifactPath");
    String entryCount = props.getProperty("entryCount");
    String minKey = props.getProperty("minKey");
    String maxKey = props.getProperty("maxKey");
    String partitionFingerprint = props.getProperty("partitionFingerprint");
    String fileCount = props.getProperty("fileCount");

    if (baseInstant == null
        || artifactPath == null
        || entryCount == null
        || minKey == null
        || maxKey == null
        || partitionFingerprint == null
        || fileCount == null) {
      LOG.info(
          "RADIX manifest missing required properties: partition={}, manifest={}, "
              + "hasBaseInstant={}, hasArtifactPath={}, hasEntryCount={}, hasMinKey={}, "
              + "hasMaxKey={}, hasFingerprint={}, hasFileCount={}",
          partitionPath,
          manifestPath,
          baseInstant != null,
          artifactPath != null,
          entryCount != null,
          minKey != null,
          maxKey != null,
          partitionFingerprint != null,
          fileCount != null);
      return null;
    }

    if (!expectedState.getLatestBaseInstant().equals(baseInstant)) {
      LOG.info(
          "RADIX manifest baseInstant mismatch: partition={}, expected={}, actual={}",
          partitionPath,
          expectedState.getLatestBaseInstant(),
          baseInstant);
      return null;
    }

    if (!expectedState.getPartitionFingerprint().equals(partitionFingerprint)) {
      LOG.info(
          "RADIX manifest fingerprint mismatch: partition={}, expected={}, actual={}",
          partitionPath,
          shortFingerprint(expectedState.getPartitionFingerprint()),
          shortFingerprint(partitionFingerprint));
      return null;
    }

    final int manifestFileCount;
    final long manifestEntryCount;
    final long manifestMinKey;
    final long manifestMaxKey;
    try {
      manifestFileCount = Integer.parseInt(fileCount);
      manifestEntryCount = Long.parseLong(entryCount);
      manifestMinKey = Long.parseLong(minKey);
      manifestMaxKey = Long.parseLong(maxKey);
    } catch (NumberFormatException nfe) {
      LOG.info(
          "RADIX manifest numeric parse failed: partition={}, manifest={}, entryCount={}, minKey={}, maxKey={}, fileCount={}",
          partitionPath,
          manifestPath,
          entryCount,
          minKey,
          maxKey,
          fileCount,
          nfe);
      return null;
    }

    if (manifestFileCount != expectedState.getFileCount()) {
      LOG.info(
          "RADIX manifest fileCount mismatch: partition={}, expected={}, actual={}",
          partitionPath,
          expectedState.getFileCount(),
          manifestFileCount);
      return null;
    }

    StoragePath artifact;
    try {
      artifact = new StoragePath(artifactPath);
    } catch (Exception e) {
      LOG.info(
          "RADIX artifact path invalid: partition={}, artifactPath={}",
          partitionPath,
          artifactPath,
          e);
      return null;
    }

    try {
      if (!storage.exists(artifact)) {
        LOG.info(
            "RADIX artifact missing: partition={}, artifactPath={}",
            partitionPath,
            artifactPath);
        return null;
      }
    } catch (IOException e) {
      LOG.info(
          "RADIX artifact existence check failed: partition={}, artifactPath={}",
          partitionPath,
          artifactPath,
          e);
      return null;
    }

    LOG.info(
        "RADIX manifest loaded successfully: partition={}, instant={}, artifact={}, entryCount={}, fileCount={}",
        partitionPath,
        baseInstant,
        artifactPath,
        manifestEntryCount,
        manifestFileCount);

    return new PartitionLookupDescriptor(
        partitionPath,
        artifactPath,
        manifestEntryCount,
        manifestMinKey,
        manifestMaxKey,
        baseInstant,
        partitionFingerprint,
        manifestFileCount);
  }

  private void persistLatestDescriptor(
      HoodieTable hoodieTable,
      PartitionLookupDescriptor descriptor) {

    HoodieStorage storage = hoodieTable.getStorage();
    StoragePath latestDir = resolveLatestManifestDir(hoodieTable);
    StoragePath manifestPath = resolveLatestManifestPath(hoodieTable, descriptor.getPartitionPath());

    try {
      storage.createDirectory(latestDir);

      Properties props = new Properties();
      props.setProperty("partitionPath", descriptor.getPartitionPath());
      props.setProperty("artifactPath", descriptor.getArtifactPath());
      props.setProperty("entryCount", Long.toString(descriptor.getEntryCount()));
      props.setProperty("minKey", Long.toString(descriptor.getMinKey()));
      props.setProperty("maxKey", Long.toString(descriptor.getMaxKey()));
      props.setProperty("baseInstant", descriptor.getBaseInstant());
      props.setProperty("partitionFingerprint", descriptor.getPartitionFingerprint());
      props.setProperty("fileCount", Integer.toString(descriptor.getFileCount()));

      try (OutputStream out = new BufferedOutputStream(storage.create(manifestPath, true))) {
        props.store(out, "RADIX latest artifact");
      }
    } catch (IOException ioe) {
      throw new RuntimeException(
          "Failed to persist RADIX latest manifest for partition=" + descriptor.getPartitionPath(),
          ioe);
    }
  }

  protected void cleanupObsoletePartitionArtifacts(
      HoodieTable hoodieTable,
      String partitionPath,
      String keepBaseInstant) {

    HoodieStorage storage = hoodieTable.getStorage();
    StoragePath instantsRoot =
        new StoragePath(resolvePersistentArtifactRoot(hoodieTable), "instants");
    try {
      if (!storage.exists(instantsRoot)) {
        return;
      }
    } catch (IOException e) {
      LOG.info("RADIX cleanup skip: instants root missing or unreadable: {}", instantsRoot, e);
      return;
    }

    String sanitizedPartition = sanitizePartition(partitionPath);

    List<StoragePathInfo> instantDirs;
    try {
      instantDirs = storage.listDirectEntries(instantsRoot);
    } catch (FileNotFoundException e) {
      return;
    } catch (IOException ioe) {
      throw new RuntimeException(
          "Failed to cleanup obsolete RADIX artifacts for partition=" + partitionPath, ioe);
    }

    for (StoragePathInfo entry : instantDirs) {
      if (!entry.isDirectory()) {
        continue;
      }
      StoragePath instantDir = entry.getPath();
      String instant = instantDir.getName();
      if (keepBaseInstant.equals(instant)) {
        continue;
      }

      StoragePath partitionDir = new StoragePath(instantDir, sanitizedPartition);
      try {
        if (!storage.exists(partitionDir)) {
          continue;
        }
        storage.deleteDirectory(partitionDir);
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to delete radix partition artifacts at " + partitionDir, e);
      }
      tryDeleteEmptyDirectory(storage, instantDir);
    }
  }

  private void tryDeleteEmptyDirectory(HoodieStorage storage, StoragePath dir) {
    try {
      if (!storage.exists(dir)) {
        return;
      }
      List<StoragePathInfo> children = storage.listDirectEntries(dir);
      if (children.isEmpty()) {
        storage.deleteDirectory(dir);
      }
    } catch (FileNotFoundException ignore) {
      // no-op
    } catch (IOException ignore) {
      // no-op
    }
  }

  protected PartitionLookupDescriptor buildPartitionLookupDescriptor(
      HoodieTable hoodieTable,
      String partitionPath) {

    long started = System.nanoTime();
    LOG.info("RADIX partition build start: partition={}", partitionPath);

    long stateStarted = System.nanoTime();
    PartitionBaseFileState state =
        resolvePartitionBaseFileState(hoodieTable, partitionPath);
    long stateElapsedMs = (System.nanoTime() - stateStarted) / 1_000_000L;

    if (state == null) {
      LOG.info("RADIX empty partition: partition={}, resolveStateMs={}", partitionPath, stateElapsedMs);
      return null;
    }

    LOG.info(
        "RADIX partition state resolved: partition={}, baseInstant={}, fingerprint={}, fileCount={}, resolveStateMs={}",
        partitionPath,
        state.getLatestBaseInstant(),
        state.getPartitionFingerprint(),
        state.getFileCount(),
        stateElapsedMs);

    long reuseCheckStarted = System.nanoTime();
    PartitionLookupDescriptor reused =
        tryLoadLatestDescriptor(hoodieTable, partitionPath, state);
    long reuseCheckElapsedMs = (System.nanoTime() - reuseCheckStarted) / 1_000_000L;

    if (reused != null) {
      long totalMs = (System.nanoTime() - started) / 1_000_000L;
      LOG.info(
          "RADIX REUSE partition={}, instant={}, artifact={}, reuseCheckMs={}, totalMs={}",
          partitionPath,
          state.getLatestBaseInstant(),
          reused.getArtifactPath(),
          reuseCheckElapsedMs,
          totalMs);
      return reused;
    }

    LOG.info(
        "RADIX REBUILD partition={}, instant={}, reuseCheckMs={}",
        partitionPath,
        state.getLatestBaseInstant(),
        reuseCheckElapsedMs);

    Path localScratchDir;
    try {
      localScratchDir = resolveLocalScratchForPartitionBuild(hoodieTable);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to resolve RADIX local scratch directory", e);
    }

    try (SpillableRadixEntrySorter sorter = createSorter(hoodieTable, localScratchDir);
         TempRadixArtifactWriter writer = createArtifactWriter(hoodieTable, localScratchDir)) {

      long streamStarted = System.nanoTime();
      streamPartitionEntries(hoodieTable, partitionPath, sorter::add);
      long streamElapsedMs = (System.nanoTime() - streamStarted) / 1_000_000L;

      LOG.info(
          "RADIX partition entries streamed: partition={}, instant={}, streamEntriesMs={}",
          partitionPath,
          state.getLatestBaseInstant(),
          streamElapsedMs);

      long mergeFinishStarted = System.nanoTime();
      try (SortedRadixEntrySource sortedEntries = sorter.finish()) {
        long mergeFinishMs = (System.nanoTime() - mergeFinishStarted) / 1_000_000L;
        long mergeSpillBytes = sorter.getMergeSpillBytesWritten();
        int mergeSpillChunks = sorter.getMergeSpillChunkCount();

        LOG.info(
            "RADIX merge/sort finished: partition={}, instant={}, mergeFinishMs={}, mergeSpillBytes={}, "
                + "mergeSpillChunks={}",
            partitionPath,
            state.getLatestBaseInstant(),
            mergeFinishMs,
            mergeSpillBytes,
            mergeSpillChunks);

        long writeStarted = System.nanoTime();
        PartitionLookupDescriptor descriptor =
            writer.write(
                partitionPath,
                state.getLatestBaseInstant(),
                sortedEntries,
                maxError,
                radixBits,
                state.getPartitionFingerprint(),
                state.getFileCount());
        long writeElapsedMs = (System.nanoTime() - writeStarted) / 1_000_000L;

        LOG.info(
            "RADIX artifact written: partition={}, instant={}, artifact={}, writeArtifactMs={}",
            partitionPath,
            descriptor.getBaseInstant(),
            descriptor.getArtifactPath(),
            writeElapsedMs);

        long persistStarted = System.nanoTime();
        persistLatestDescriptor(hoodieTable, descriptor);
        long persistElapsedMs = (System.nanoTime() - persistStarted) / 1_000_000L;

        LOG.info(
            "RADIX manifest persisted: partition={}, instant={}, persistManifestMs={}",
            partitionPath,
            descriptor.getBaseInstant(),
            persistElapsedMs);

        long cleanupMs;
        long cleanupStarted = System.nanoTime();
        try {
          cleanupObsoletePartitionArtifacts(
              hoodieTable,
              partitionPath,
              descriptor.getBaseInstant());
          cleanupMs = (System.nanoTime() - cleanupStarted) / 1_000_000L;
          LOG.info(
              "RADIX obsolete artifacts cleaned: partition={}, keepBaseInstant={}, cleanupMs={}",
              partitionPath,
              state.getLatestBaseInstant(),
              cleanupMs);
        } catch (RuntimeException cleanupFailure) {
          cleanupMs = (System.nanoTime() - cleanupStarted) / 1_000_000L;
          LOG.warn(
              "RADIX cleanup failed: partition={}, keepBaseInstant={}, cleanupMs={}",
              partitionPath,
              state.getLatestBaseInstant(),
              cleanupMs,
              cleanupFailure);
        }

        long totalMs = (System.nanoTime() - started) / 1_000_000L;
        LOG.info(
            "RADIX partition rebuild complete: partition={}, instant={}, totalMs={}, resolveStateMs={}, "
                + "reuseCheckMs={}, streamEntriesMs={}, mergeFinishMs={}, mergeSpillBytes={}, "
                + "mergeSpillChunks={}, writeArtifactMs={}, persistManifestMs={}, cleanupMs={}",
            partitionPath,
            descriptor.getBaseInstant(),
            totalMs,
            stateElapsedMs,
            reuseCheckElapsedMs,
            streamElapsedMs,
            mergeFinishMs,
            mergeSpillBytes,
            mergeSpillChunks,
            writeElapsedMs,
            persistElapsedMs,
            cleanupMs);

        return descriptor;
      }
    } catch (EmptyPartitionBuildException empty) {
      LOG.info("RADIX empty partition after streaming: partition={}", partitionPath);
      return null;
    } catch (IOException ioe) {
      throw new RuntimeException(
          "Failed to build persistent radix artifact for partition=" + partitionPath, ioe);
    }
  }

  /**
   * @param localScratchDir directory for merge spill files (same path as writer temp files in production)
   */
  protected SpillableRadixEntrySorter createSorter(HoodieTable hoodieTable, Path localScratchDir) {
    return new ExternalMergeRadixEntrySorter(
        localScratchDir, config.getRadixSplineMergeMaxEntriesInMemory());
  }

  /**
   * @param localScratchDir directory for radix writer temp files (entries/keys/offsets)
   */
  protected TempRadixArtifactWriter createArtifactWriter(
      HoodieTable hoodieTable, Path localScratchDir) {
    return new SimpleTempRadixArtifactWriter(
        new StorageRadixArtifactPublisher(
            hoodieTable.getStorage(),
            resolvePersistentArtifactRoot(hoodieTable)),
        config.getRadixSplineMaxEntriesPerPartition(),
        localScratchDir);
  }

  private List<HoodieBaseFile> getReusableBaseFiles(HoodieTable hoodieTable, String partitionPath) {
    List<HoodieBaseFile> baseFiles = new ArrayList<>();

    hoodieTable.getMetaClient()
        .getCommitsTimeline()
        .filterCompletedInstants()
        .lastInstant()
        .ifPresent(instant ->
            hoodieTable.getBaseFileOnlyView()
                .getLatestBaseFilesBeforeOrOn(partitionPath, instant.requestedTime())
                .forEach(baseFiles::add)
        );

    return baseFiles;
  }

  protected void streamPartitionEntries(
      HoodieTable hoodieTable,
      String partitionPath,
      RadixIOConsumer<RadixLocationEntry> consumer) throws IOException {

    ensureKeyEncoderInitialized();

    List<HoodieBaseFile> baseFiles = getReusableBaseFiles(hoodieTable, partitionPath);

    if (baseFiles.isEmpty()) {
      return;
    }

    for (HoodieBaseFile baseFile : baseFiles) {
      Pair<String, HoodieBaseFile> partitionBaseFilePair = Pair.of(partitionPath, baseFile);

      HoodieKeyLocationFetchHandle fetchHandle = new HoodieKeyLocationFetchHandle(
          config,
          hoodieTable,
          partitionBaseFilePair,
          Option.empty()
      );

      try (ClosableIterator<Pair<HoodieKey, HoodieRecordLocation>> it = fetchHandle.locations()) {
        while (it.hasNext()) {
          Pair<HoodieKey, HoodieRecordLocation> entry = it.next();
          HoodieKey hoodieKey = entry.getLeft();

          if (!partitionPath.equals(hoodieKey.getPartitionPath())) {
            continue;
          }

          String recordKey = hoodieKey.getRecordKey();
          long encodedKey = encodeRecordKeyOrThrow(recordKey, partitionPath, "streamPartitionEntries");
          consumer.accept(new RadixLocationEntry(encodedKey, recordKey, entry.getRight()));
        }
      }
    }
  }

  private long encodeRecordKeyOrThrow(String recordKey, String partitionPath, String stage) {
    try {
      return keyEncoder.encode(recordKey);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "RADIX_SPLINE invalid record key for recordkey.field='" + recordKeyField
              + "', fieldType=" + recordKeyFieldTypeDescription
              + ", partition=" + partitionPath
              + ", stage=" + stage
              + ", recordKey='" + recordKey + "': "
              + e.getMessage(),
          e);
    }
  }

  private static long elapsedMs(long startedNanos) {
    return (System.nanoTime() - startedNanos) / 1_000_000L;
  }

  private Map<String, PartitionLookupRuntime> buildRuntimeMap(
      List<PartitionLookupDescriptor> descriptors) {
    Map<String, PartitionLookupRuntime> runtimeByPartition =
        new HashMap<>(hashMapCapacityForExpectedSize(descriptors.size()));
    for (PartitionLookupDescriptor descriptor : descriptors) {
      runtimeByPartition.put(
          descriptor.getPartitionPath(),
          new PartitionLookupRuntime(descriptor, radixLookupWindowParams));
    }
    return runtimeByPartition;
  }

  private void cleanupPartitionLookupRuntimes(
      Map<String, PartitionLookupRuntime> runtimeByPartition) {
    if (runtimeByPartition == null || runtimeByPartition.isEmpty()) {
      return;
    }

    long cleanupStartedNanos = System.nanoTime();

    for (PartitionLookupRuntime runtime : runtimeByPartition.values()) {
      if (runtime == null) {
        continue;
      }
      runtime.closeReaderQuietly();
    }

    LOG.info(
        "RADIX tagLocation runtime cleanup complete: partitions={}, cleanupMs={}",
        runtimeByPartition.size(),
        elapsedMs(cleanupStartedNanos));
  }

  private void ensureKeyEncoderInitialized() {
    if (keyEncoder != null) {
      return;
    }

    synchronized (this) {
      if (keyEncoder != null) {
        return;
      }

      String keyField = config.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME);
      if (keyField == null || keyField.trim().isEmpty()) {
        throw new IllegalArgumentException(
            "RADIX_SPLINE requires recordkey.field to be configured");
      }
      if (keyField.contains(",")) {
        throw new IllegalArgumentException(
            "RADIX_SPLINE currently supports only a single recordkey.field, but got: " + keyField);
      }

      String schemaStr = config.getWriteSchema();
      if (schemaStr == null || schemaStr.trim().isEmpty()) {
        throw new IllegalArgumentException(
            "RADIX_SPLINE requires write schema to be present in HoodieWriteConfig");
      }

      Schema writeSchema = new Schema.Parser().parse(schemaStr);
      Schema fieldSchema = resolveFieldSchema(writeSchema, keyField);

      this.recordKeyField = keyField;
      this.recordKeyFieldTypeDescription = describeSchema(fieldSchema);
      this.keyEncoder = buildEncoderForFieldSchema(keyField, fieldSchema);
    }
  }

  private static RadixSplineKeyEncoder buildEncoderForFieldSchema(String fieldPath, Schema schema) {
    Schema effective = unwrapNullableUnion(schema);

    switch (effective.getType()) {
      case INT:
      case LONG:
        rejectLogicalTypes(fieldPath, effective);
        return new RadixSplineKeyEncoder(RadixSplineKeyEncoder.Mode.NUMERIC_COLUMN);

      case STRING:
        return new RadixSplineKeyEncoder(RadixSplineKeyEncoder.Mode.STRING_DECIMAL_COLUMN);

      default:
        throw new IllegalArgumentException(
            "RADIX_SPLINE supports only integer-like or decimal-string record keys. "
                + "recordkey.field='" + fieldPath + "' has unsupported type: " + describeSchema(effective)
                + ". Materialize a numeric key column and use it as recordkey.field.");
    }
  }

  private static void rejectLogicalTypes(String fieldPath, Schema schema) {
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      throw new IllegalArgumentException(
          "RADIX_SPLINE does not directly support logical type record keys. "
              + "recordkey.field='" + fieldPath + "' has logical type: " + logicalType.getName()
              + " (" + describeSchema(schema) + "). "
              + "Materialize a numeric key column and use it as recordkey.field.");
    }
  }

  private static Schema resolveFieldSchema(Schema rootSchema, String fieldPath) {
    String[] parts = fieldPath.split("\\.");
    Schema current = rootSchema;

    for (String part : parts) {
      current = unwrapNullableUnion(current);

      if (current.getType() != Schema.Type.RECORD) {
        throw new IllegalArgumentException(
            "RADIX_SPLINE could not resolve recordkey.field='" + fieldPath
                + "': path segment '" + part + "' is not inside a RECORD schema, actual type="
                + describeSchema(current));
      }

      Schema.Field field = current.getField(part);
      if (field == null) {
        throw new IllegalArgumentException(
            "RADIX_SPLINE could not resolve recordkey.field='" + fieldPath
                + "': field '" + part + "' not found in schema " + current.getFullName());
      }

      current = field.schema();
    }

    return unwrapNullableUnion(current);
  }

  private static Schema unwrapNullableUnion(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      return schema;
    }

    List<Schema> nonNullSchemas = new ArrayList<>();
    for (Schema member : schema.getTypes()) {
      if (member.getType() != Schema.Type.NULL) {
        nonNullSchemas.add(member);
      }
    }

    if (nonNullSchemas.size() != 1) {
      throw new IllegalArgumentException(
          "RADIX_SPLINE does not support complex union schemas for record key fields: " + schema);
    }

    return nonNullSchemas.get(0);
  }

  private static String describeSchema(Schema schema) {
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType == null) {
      return schema.getType().name();
    }
    return schema.getType().name() + "(logicalType=" + logicalType.getName() + ")";
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(
      HoodieData<WriteStatus> writeStatuses,
      HoodieEngineContext context,
      HoodieTable hoodieTable) {
    return writeStatuses;
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    if (instantTime == null || instantTime.isEmpty()) {
      return true;
    }
    try {
      StorageConfiguration<Configuration> storageConf = newRollbackStorageConf();
      HoodieStorage storage = HoodieStorageUtils.getStorage(config.getBasePath(), storageConf);
      StoragePath radixRoot = new StoragePath(config.getBasePath(), ".hoodie/.radix_index_tmp");
      deleteRadixInstantStaging(storage, radixRoot, instantTime);
      RadixArtifactReaderCache.evictArtifactsUnderRadixInstant(instantTime);
      removeLatestManifestsReferencingInstant(storage, radixRoot, instantTime);
    } catch (Exception e) {
      // Best-effort: data rollback has already completed; fingerprint checks on next tagLocation
      // revalidate or rebuild descriptors if manifests/artifacts are inconsistent.
      LOG.warn(
          "RADIX rollback staging cleanup failed (non-fatal) for instant={}, basePath={}",
          instantTime,
          config.getBasePath(),
          e);
    }
    return true;
  }

  private StorageConfiguration<Configuration> newRollbackStorageConf() {
    Configuration hconf;
    scala.Option<SparkSession> active = SparkSession.getActiveSession();
    if (active.isDefined()) {
      hconf = new Configuration(active.get().sparkContext().hadoopConfiguration());
    } else {
      hconf = HadoopFSUtils.prepareHadoopConf(new Configuration());
    }
    config.getProps().forEach((k, v) -> {
      if (k != null && v != null) {
        hconf.set(k.toString(), v.toString());
      }
    });
    return new HadoopStorageConfiguration(hconf, true);
  }

  private static void deleteRadixInstantStaging(
      HoodieStorage storage,
      StoragePath radixRoot,
      String instantTime) throws IOException {

    StoragePath instantDir = new StoragePath(new StoragePath(radixRoot, "instants"), instantTime);
    if (!storage.exists(instantDir)) {
      return;
    }
    storage.deleteDirectory(instantDir);
    LOG.info("RADIX rollback removed staging directory: {}", instantDir);
  }

  private static void removeLatestManifestsReferencingInstant(
      HoodieStorage storage,
      StoragePath radixRoot,
      String instantTime) throws IOException {

    StoragePath latestDir = new StoragePath(radixRoot, "latest");
    if (!storage.exists(latestDir)) {
      return;
    }

    List<StoragePathInfo> entries;
    try {
      entries = storage.listDirectEntries(latestDir);
    } catch (FileNotFoundException e) {
      return;
    }

    String instantSegment = "/instants/" + instantTime + "/";
    for (StoragePathInfo entry : entries) {
      if (entry.isDirectory()) {
        continue;
      }
      StoragePath manifestPath = entry.getPath();
      if (!manifestPath.getName().endsWith(".properties")) {
        continue;
      }

      Properties props = new Properties();
      try (InputStream in = new BufferedInputStream(storage.open(manifestPath))) {
        props.load(in);
      } catch (IOException e) {
        LOG.info("RADIX rollback skip unreadable manifest: {}", manifestPath, e);
        continue;
      }

      String baseInstant = props.getProperty("baseInstant");
      String artifactPath = props.getProperty("artifactPath");
      boolean drop =
          instantTime.equals(baseInstant)
              || (artifactPath != null && artifactPath.contains(instantSegment));
      if (!drop) {
        continue;
      }

      if (artifactPath != null) {
        RadixArtifactReaderCache.evict(artifactPath);
      }
      if (storage.deleteFile(manifestPath)) {
        LOG.info(
            "RADIX rollback removed latest manifest for rolled-back instant={}: {}",
            instantTime,
            manifestPath);
      }
    }
  }

  @Override
  public boolean requiresTagging(WriteOperationType operationType) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }

  @Override
  public String toString() {
    return "HoodieRadixSplineIndex{"
        + "maxError=" + maxError
        + ", radixBits=" + radixBits
        + ", recordKeyField=" + recordKeyField
        + ", recordKeyFieldType=" + recordKeyFieldTypeDescription
        + '}';
  }

  @Override
  public void close() {
    RadixArtifactReaderCache.evictForTable(config.getBasePath());
  }

  private void validateNoDuplicateEncodedKeys(
      String partitionPath,
      List<RadixLocationEntry> entries) {
    for (int i = 1; i < entries.size(); i++) {
      RadixLocationEntry prev = entries.get(i - 1);
      RadixLocationEntry cur = entries.get(i);

      if (prev.getEncodedKey() == cur.getEncodedKey()) {
        throw new IllegalStateException(
            "RADIX_SPLINE duplicate encodedKey detected in partition=" + partitionPath
                + ", encodedKey=" + cur.getEncodedKey()
                + ", prevRecordKey=" + prev.getRecordKey()
                + ", currRecordKey=" + cur.getRecordKey()
                + ", recordkey.field=" + recordKeyField
                + ", fieldType=" + recordKeyFieldTypeDescription);
      }
    }
  }

  /**
   * Internal signal for "partition had no entries, no artifact needs to be built".
   */
  static final class EmptyPartitionBuildException extends RuntimeException {
    EmptyPartitionBuildException(String message) {
      super(message);
    }
  }
}