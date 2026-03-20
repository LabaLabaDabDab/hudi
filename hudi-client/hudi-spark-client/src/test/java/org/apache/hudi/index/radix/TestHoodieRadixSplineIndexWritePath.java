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

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieRadixSplineIndexWritePath extends HoodieSparkClientTestHarness {

  private HoodieTestDataGenerator dataGen;

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initHoodieStorage();
    initMetaClient();
    dataGen = new HoodieTestDataGenerator(0xBEEFL);
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void testBulkInsertAndUpsertWithRadixSplineIndex() throws Exception {
    HoodieWriteConfig config = makeConfig();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context, config)) {
      List<HoodieRecord> inserts = dataGen.generateInserts("000", 100);

      String instant1 = client.startCommit();
      List<WriteStatus> bulkInsertStatuses =
          client.bulkInsert(jsc.parallelize(inserts, 1), instant1).collect();
      assertNoWriteErrors(bulkInsertStatuses);

      List<HoodieRecord> updates = dataGen.generateUpdates(instant1, 100);

      String instant2 = client.startCommit();
      List<WriteStatus> upsertStatuses =
          client.upsert(jsc.parallelize(updates, 1), instant2).collect();
      assertNoWriteErrors(upsertStatuses);
    }
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