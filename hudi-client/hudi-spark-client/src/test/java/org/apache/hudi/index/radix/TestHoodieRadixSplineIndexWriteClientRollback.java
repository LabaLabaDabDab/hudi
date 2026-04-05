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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration checks: {@link SparkRDDWriteClient#rollback(String)} invokes index rollback and the table
 * remains writable afterward. On-disk radix layout under {@code .hoodie/.radix_index_tmp} is asserted in
 * {@link TestHoodieRadixSplineIndexRollback}.
 */
public class TestHoodieRadixSplineIndexWriteClientRollback extends HoodieSparkClientTestHarness {

  private HoodieTestDataGenerator dataGen;

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initHoodieStorage();
    initMetaClient();
    dataGen = new HoodieTestDataGenerator(0xBEEFL);
    RadixArtifactReaderCache.clear();
  }

  @AfterEach
  public void tearDown() throws Exception {
    RadixArtifactReaderCache.clear();
    cleanupResources();
  }

  @Test
  public void testRollbackLastCommitThenUpsertSucceeds() throws Exception {
    HoodieWriteConfig config = makeConfig();

    String instant1;
    String instant2;
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context, config)) {
      List<HoodieRecord> inserts = dataGen.generateInserts("000", 60);
      instant1 = client.startCommit();
      List<WriteStatus> bulk =
          client.bulkInsert(jsc.parallelize(inserts, 1), instant1).collect();
      assertNoWriteErrors(bulk);

      List<HoodieRecord> updates = dataGen.generateUpdates(instant1, 60);
      instant2 = client.startCommit();
      List<WriteStatus> upserted =
          client.upsert(jsc.parallelize(updates, 1), instant2).collect();
      assertNoWriteErrors(upserted);

      Path inst2 = radixInstantDir(instant2);

      client.rollback(instant2);

      assertFalse(
          Files.exists(inst2),
          "radix staging dir for rolled-back instant must not remain: " + inst2);

      List<HoodieRecord> updates2 = dataGen.generateUpdates(instant1, 60);
      String instant3 = client.startCommit();
      List<WriteStatus> afterRollback =
          client.upsert(jsc.parallelize(updates2, 1), instant3).collect();
      assertNoWriteErrors(afterRollback);
    }
  }

  private Path radixInstantDir(String instantTime) {
    return Path.of(basePath, ".hoodie", ".radix_index_tmp", "instants", instantTime);
  }

  private HoodieWriteConfig makeConfig() {
    Properties props = new Properties();
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(1, 1)
        .withDeleteParallelism(1)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.RADIX_SPLINE)
            .build())
        .build();
  }

  private void assertNoWriteErrors(List<WriteStatus> statuses) {
    assertFalse(statuses.isEmpty(), "write statuses should not be empty");
    assertTrue(
        statuses.stream().noneMatch(WriteStatus::hasErrors),
        "write should complete without errors");
  }
}
