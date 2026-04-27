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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Manual comparison of index types at increasing row counts: bulk insert + upsert (tag location),
 * printed table with on-disk sizes (total, .hoodie, RADIX staging, data approximation).
 *
 * <p>Enable: {@code -Dbenchmark.enabled=true}
 *
 * <p>Optional:
 *
 * <ul>
 *   <li>{@code -Dbenchmark.scales=100000,500000,1000000} — comma-separated row counts (default
 *       100k … 10M).
 *   <li>{@code -Dbenchmark.indexes=BLOOM,SIMPLE,RADIX_SPLINE} — index types to run; {@code ALL}
 *       expands to Spark-backed index types ({@code INMEMORY}, bloom/simple variants, record-level,
 *       {@code RADIX_SPLINE}). {@code BUCKET}
 *       is omitted from {@code ALL} when Spark SQL adapter classes from {@code hudi-spark3.x}
 *       are not on the test classpath (bucket bulk-insert requires them).
 *   <li>{@code -Dbenchmark.maxScale=1000000} — skip scales greater than this (safety cap).
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IndexScaleComparisonBenchmark extends HoodieSparkClientTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(IndexScaleComparisonBenchmark.class);

  /** Minimal trip-like schema with partition_path for SimpleKeyGenerator. */
  private static final String BENCH_SCHEMA =
      "{\"type\":\"record\",\"name\":\"benchTrip\",\"fields\":["
          + "{\"name\":\"timestamp\",\"type\":\"long\"},"
          + "{\"name\":\"_row_key\",\"type\":\"string\"},"
          + "{\"name\":\"partition_path\",\"type\":\"string\"},"
          + "{\"name\":\"rider\",\"type\":\"string\"},"
          + "{\"name\":\"driver\",\"type\":\"string\"},"
          + "{\"name\":\"fare\",\"type\":\"double\"},"
          + "{\"name\":\"_hoodie_is_deleted\",\"type\":\"boolean\",\"default\":false}]}";

  private static final Schema AVRO = new Schema.Parser().parse(BENCH_SCHEMA);
  private static final String PARTITION = "2016/03/15";

  @BeforeAll
  void startSpark() throws Exception {
    initSparkContexts();
  }

  @AfterAll
  void stopSpark() throws Exception {
    cleanupSparkContexts();
  }

  @Test
  @EnabledIfSystemProperty(named = "benchmark.enabled", matches = "true")
  void runComparison() throws Exception {
    long[] scales = parseScales(System.getProperty("benchmark.scales"));
    List<HoodieIndex.IndexType> types = parseIndexTypes(System.getProperty("benchmark.indexes"));
    long maxScale = Long.getLong("benchmark.maxScale", Long.MAX_VALUE);

    System.out.println();
    System.out.println(
        "=== Index scale benchmark (bulk insert + upsert for tagLocation) ===");
    System.out.println(
        "Scales: "
            + LongStream.of(scales)
                .filter(s -> s <= maxScale)
                .mapToObj(Long::toString)
                .collect(Collectors.joining(", ")));
    System.out.println(
        "Indexes: "
            + types.stream().map(Enum::name).collect(Collectors.joining(", ")));
    System.out.println();

    printHeader();

    for (HoodieIndex.IndexType indexType : types) {
      for (long scale : scales) {
        if (scale > maxScale) {
          LOG.info("Skip scale {} > benchmark.maxScale {}", scale, maxScale);
          continue;
        }
        runOneScenario(indexType, scale);
      }
    }
    System.out.println();
  }

  private void runOneScenario(HoodieIndex.IndexType indexType, long scale) throws Exception {
    String folder = indexType.name() + "_" + scale;
    wipeIfExists(tempDir.resolve(folder));
    initPath(folder);
    initHoodieStorage();
    initMetaClient(HoodieTableType.COPY_ON_WRITE);

    HoodieWriteConfig cfg = buildConfig(indexType, scale);
    int sparkParts = sparkPartitions(scale);

    long t0 = System.nanoTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context, cfg)) {
      JavaRDD<HoodieRecord> insertRdd =
          sparkSession
              .range(0L, scale, 1L, sparkParts)
              .toJavaRDD()
              .map(IndexScaleComparisonBenchmark::benchInsertRow);

      String instant1 = client.startCommit();
      List<WriteStatus> st1 = client.bulkInsert(insertRdd, instant1).collect();
      assertNoErrors(st1);

      JavaRDD<HoodieRecord> updateRdd =
          sparkSession
              .range(0L, scale, 1L, sparkParts)
              .toJavaRDD()
              .map(IndexScaleComparisonBenchmark::benchUpdateRow);

      String instant2 = client.startCommit();
      List<WriteStatus> st2 = client.upsert(updateRdd, instant2).collect();
      assertNoErrors(st2);
    }
    double wallSec = (System.nanoTime() - t0) / 1_000_000_000.0;

    Path table = Path.of(basePath);
    long total = sizeDir(table);
    Path hoodie = table.resolve(".hoodie");
    long hoodieBytes = Files.exists(hoodie) ? sizeDir(hoodie) : 0L;
    Path radix = hoodie.resolve(".radix_index_tmp");
    long radixBytes = Files.exists(radix) ? sizeDir(radix) : 0L;
    long dataApprox = Math.max(0L, total - hoodieBytes);

    printRow(indexType.name(), scale, wallSec, total, hoodieBytes, radixBytes, dataApprox);

    cleanupResourcesPartial();
    metaClient = null;
  }

  /** Between scenarios: reset FS without tearing down Spark. */
  private void cleanupResourcesPartial() throws IOException {
    cleanupTimelineService();
    cleanupClients();
    cleanupFileSystem();
    System.gc();
  }

  private static void printHeader() {
    System.out.printf(
        Locale.ROOT,
        "%-14s %12s %12s %12s %12s %12s %12s%n",
        "index",
        "rows",
        "wall_sec",
        "total_mb",
        "hoodie_mb",
        "radix_tmp_mb",
        "data_mb*");
    System.out.println(
        "-------------- ------------ ------------ ------------ ------------ ------------ ------------");
    System.out.println(
        "* data_mb ≈ total - .hoodie (parquet/ORC under partition paths; bloom filters live inside data files).");
    System.out.println();
  }

  private static void printRow(
      String index,
      long rows,
      double wallSec,
      long total,
      long hoodie,
      long radix,
      long dataApprox) {
    System.out.printf(
        Locale.ROOT,
        "%-14s %12d %12.2f %12.2f %12.2f %12.2f %12.2f%n",
        index,
        rows,
        wallSec,
        toMb(total),
        toMb(hoodie),
        toMb(radix),
        toMb(dataApprox));
  }

  private static double toMb(long bytes) {
    return bytes / (1024.0 * 1024.0);
  }

  private HoodieWriteConfig buildConfig(HoodieIndex.IndexType indexType, long scale) {
    Properties props = new Properties();
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");

    HoodieIndexConfig.Builder indexBuilder =
        HoodieIndexConfig.newBuilder().fromProperties(props).withIndexType(indexType);
    if (indexType == HoodieIndex.IndexType.BUCKET) {
      indexBuilder
          .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE)
          .withBucketNum("8");
    }

    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(BENCH_SCHEMA)
        .forTable("index_scale_bench")
        .withParallelism(sparkPartitions(scale), sparkPartitions(scale))
        .withDeleteParallelism(1)
        .withProps(props)
        .withMetadataConfig(buildMetadataForIndexType(indexType))
        .withIndexConfig(indexBuilder.build())
        .build();
  }

  private static HoodieMetadataConfig buildMetadataForIndexType(HoodieIndex.IndexType indexType) {
    switch (indexType) {
      case RECORD_INDEX:
      case GLOBAL_RECORD_LEVEL_INDEX:
        return HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableGlobalRecordLevelIndex(true)
            .build();
      case RECORD_LEVEL_INDEX:
        Properties mp = new Properties();
        mp.setProperty(HoodieMetadataConfig.ENABLE.key(), "true");
        mp.setProperty(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
        return HoodieMetadataConfig.newBuilder().fromProperties(mp).build();
      default:
        return HoodieMetadataConfig.newBuilder().enable(false).build();
    }
  }

  private static int sparkPartitions(long scale) {
    int p = (int) Math.max(4L, scale / 100_000L);
    return Math.min(512, p);
  }

  /** Static for Spark closure serialization (no outer {@code this}). */
  private static HoodieRecord benchInsertRow(long i) {
    return new HoodieAvroRecord<>(
        new HoodieKey(String.valueOf(i), PARTITION),
        new HoodieAvroPayload(Option.of(genericRow(i, 1.0))));
  }

  private static HoodieRecord benchUpdateRow(long i) {
    return new HoodieAvroRecord<>(
        new HoodieKey(String.valueOf(i), PARTITION),
        new HoodieAvroPayload(Option.of(genericRow(i, 2.0))));
  }

  private static GenericRecord genericRow(long i, double fare) {
    GenericRecord gr = new GenericData.Record(AVRO);
    gr.put("timestamp", i);
    gr.put("_row_key", String.valueOf(i));
    gr.put("partition_path", PARTITION);
    gr.put("rider", "r");
    gr.put("driver", "d");
    gr.put("fare", fare);
    gr.put("_hoodie_is_deleted", false);
    return gr;
  }

  private static void assertNoErrors(List<WriteStatus> statuses) {
    assertFalse(statuses.stream().anyMatch(WriteStatus::hasErrors), "write errors");
  }

  private static long[] parseScales(String prop) {
    if (prop == null || prop.isBlank()) {
      return new long[] {100_000L, 500_000L, 1_000_000L, 5_000_000L, 10_000_000L};
    }
    String[] parts = prop.split(",");
    long[] out = new long[parts.length];
    for (int i = 0; i < parts.length; i++) {
      out[i] = Long.parseLong(parts[i].trim());
    }
    return out;
  }

  private List<HoodieIndex.IndexType> parseIndexTypes(String prop) {
    if (prop == null || prop.isBlank()) {
      List<HoodieIndex.IndexType> d = new ArrayList<>(3);
      d.add(HoodieIndex.IndexType.BLOOM);
      d.add(HoodieIndex.IndexType.SIMPLE);
      d.add(HoodieIndex.IndexType.RADIX_SPLINE);
      return d;
    }
    if ("ALL".equalsIgnoreCase(prop.trim())) {
      return allSparkIndexTypes();
    }
    List<HoodieIndex.IndexType> out = new ArrayList<>();
    for (String p : prop.split(",")) {
      out.add(HoodieIndex.IndexType.valueOf(p.trim()));
    }
    return out;
  }

  /**
   * All index types that {@link org.apache.hudi.index.SparkHoodieIndexFactory} can materialize
   * (excludes {@code FLINK_STATE} and other non-Spark engines).
   *
   * <p>{@link HoodieIndex.IndexType#BUCKET} is included only when the Spark SQL adapter class for
   * the current Spark version is on the classpath (from {@code hudi-spark3.x} / {@code
   * hudi-spark4.x}); without it, bucket bulk insert fails at {@code RDDBucketIndexPartitioner}.
   */
  private List<HoodieIndex.IndexType> allSparkIndexTypes() {
    ArrayList<HoodieIndex.IndexType> list =
        new ArrayList<>(
            Arrays.asList(
                HoodieIndex.IndexType.INMEMORY,
                HoodieIndex.IndexType.BLOOM,
                HoodieIndex.IndexType.GLOBAL_BLOOM,
                HoodieIndex.IndexType.SIMPLE,
                HoodieIndex.IndexType.GLOBAL_SIMPLE,
                HoodieIndex.IndexType.BUCKET,
                HoodieIndex.IndexType.RECORD_INDEX,
                HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX,
                HoodieIndex.IndexType.RECORD_LEVEL_INDEX,
                HoodieIndex.IndexType.RADIX_SPLINE));
    if (!isSparkSqlAdapterOnClasspath()) {
      boolean removed = list.remove(HoodieIndex.IndexType.BUCKET);
      if (removed) {
        LOG.warn(
            "Skipping BUCKET in ALL: Spark SQL adapter class {} not on classpath "
                + "(build hudi-spark3.x and add jar, or run BUCKET explicitly from a module that depends on it).",
            expectedSparkSqlAdapterClass());
      }
    }
    return Collections.unmodifiableList(list);
  }

  /** Mirrors {@link org.apache.hudi.SparkAdapterSupport} adapter class selection. */
  static String expectedSparkSqlAdapterClass(String sparkVersion) {
    if (sparkVersion.startsWith("4.0")) {
      return "org.apache.spark.sql.adapter.Spark4_0Adapter";
    }
    if (sparkVersion.startsWith("3.5")) {
      return "org.apache.spark.sql.adapter.Spark3_5Adapter";
    }
    if (sparkVersion.startsWith("3.4")) {
      return "org.apache.spark.sql.adapter.Spark3_4Adapter";
    }
    return "org.apache.spark.sql.adapter.Spark3_3Adapter";
  }

  private boolean isSparkSqlAdapterOnClasspath() {
    try {
      Class.forName(expectedSparkSqlAdapterClass(sparkSession.sparkContext().version()));
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  private String expectedSparkSqlAdapterClass() {
    return expectedSparkSqlAdapterClass(sparkSession.sparkContext().version());
  }

  private static long sizeDir(Path root) throws IOException {
    if (!Files.exists(root)) {
      return 0L;
    }
    try (Stream<Path> walk = Files.walk(root)) {
      return walk.filter(Files::isRegularFile).mapToLong(IndexScaleComparisonBenchmark::safeSize).sum();
    }
  }

  private static long safeSize(Path p) {
    try {
      return Files.size(p);
    } catch (IOException e) {
      return 0L;
    }
  }

  private void wipeIfExists(Path dir) throws IOException {
    if (!Files.exists(dir)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(dir)) {
      walk.sorted(Comparator.reverseOrder()).forEach(IndexScaleComparisonBenchmark::deleteQuiet);
    }
  }

  private static void deleteQuiet(Path p) {
    try {
      Files.deleteIfExists(p);
    } catch (IOException e) {
      LOG.debug("deleteIfExists failed: {} -> {}", p, e.toString());
    }
  }
}
