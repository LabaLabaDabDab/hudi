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
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Ensures {@link HoodieRadixSplineIndex#loadPartitionLookups(org.apache.hudi.common.data.HoodieData,
 * org.apache.hudi.table.HoodieTable)} derives the partition set from record {@link HoodieRecord#getPartitionPath()}
 * values (unique), not from whole-table partition enumeration.
 */
public class TestHoodieRadixSplineIndexLoadPartitionLookups {

  @TempDir
  Path tempDir;

  private HoodieWriteConfig config;

  @BeforeEach
  public void setUp() {
    config =
        HoodieWriteConfig.newBuilder()
            .withPath(tempDir.toUri().toString())
            .forTable("radix_load_partition_lookups_test")
            .withSchema(
                "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}")
            .withProps(
                Collections.singletonMap(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id"))
            .withIndexConfig(
                HoodieIndexConfig.newBuilder()
                    .withIndexType(HoodieIndex.IndexType.RADIX_SPLINE)
                    .build())
            .build();
  }

  @Test
  public void lookupPartitionPathsMatchUniquePathsFromRecords() {
    StubDescriptorIndex index = new StubDescriptorIndex(config);

    HoodieAvroRecord<EmptyHoodieRecordPayload> r1 = plainRecord("a", "part/x");
    HoodieAvroRecord<EmptyHoodieRecordPayload> r2 = plainRecord("b", "part/x");
    HoodieAvroRecord<EmptyHoodieRecordPayload> r3 = plainRecord("c", "part/y");

    HoodieData<HoodieRecord<EmptyHoodieRecordPayload>> data =
        HoodieListData.eager(Arrays.asList(r1, r2, r3));

    Set<String> expectedUnique = new HashSet<>(Arrays.asList("part/x", "part/y"));

    Set<String> actual =
        index.partitionPathsForLookup(data, null).stream().collect(Collectors.toSet());

    assertEquals(expectedUnique, actual);
  }

  @Test
  public void emptyRecordsYieldNoPartitions() {
    StubDescriptorIndex index = new StubDescriptorIndex(config);

    HoodieData<HoodieRecord<EmptyHoodieRecordPayload>> data =
        HoodieListData.eager(Collections.emptyList());

    List<String> paths = index.partitionPathsForLookup(data, null);
    assertTrue(paths.isEmpty());
  }

  private static HoodieAvroRecord<EmptyHoodieRecordPayload> plainRecord(
      String recordKey, String partitionPath) {
    return new HoodieAvroRecord<>(
        new HoodieKey(recordKey, partitionPath), new EmptyHoodieRecordPayload());
  }

  /** Supplies trivial descriptors so {@link #loadPartitionLookups} does not touch storage. */
  private static final class StubDescriptorIndex extends HoodieRadixSplineIndex {

    StubDescriptorIndex(HoodieWriteConfig writeConfig) {
      super(writeConfig);
    }

    <R extends HoodieRecordPayload> List<String> partitionPathsForLookup(
        HoodieData<HoodieRecord<R>> records, org.apache.hudi.table.HoodieTable hoodieTable) {
      return loadPartitionLookups(records, hoodieTable).stream()
          .map(PartitionLookupDescriptor::getPartitionPath)
          .collect(Collectors.toList());
    }

    @Override
    protected PartitionLookupDescriptor buildPartitionLookupDescriptor(
        org.apache.hudi.table.HoodieTable hoodieTable, String partitionPath) {
      return new PartitionLookupDescriptor(
          partitionPath, "file:///unused-artifact", 1L, 0L, 1L, "1", "fp", 1);
    }
  }
}
