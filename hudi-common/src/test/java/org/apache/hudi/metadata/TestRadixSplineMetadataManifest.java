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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieRadixSplineIndexManifest;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.RADIX_SPLINE_INDEX_TYPE_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.TABLE_INDEX_TYPE_PROP;
import static org.apache.hudi.metadata.HoodieIndexVersion.V1;

public class TestRadixSplineMetadataManifest {

  @Test
  public void propertiesToManifestMatchesWriterKeys() {
    Properties props = new Properties();
    props.setProperty("partitionPath", "p1");
    props.setProperty("artifactPath", "file:///tmp/x.bin");
    props.setProperty("entryCount", "10");
    props.setProperty("minKey", "1");
    props.setProperty("maxKey", "99");
    props.setProperty("baseInstant", "20240101120000");
    props.setProperty("partitionFingerprint", "abc");
    props.setProperty("fileCount", "3");

    Option<HoodieRadixSplineIndexManifest> opt = HoodieTableMetadataUtil.propertiesToRadixSplineManifest(props);
    assertTrue(opt.isPresent());
    HoodieRadixSplineIndexManifest m = opt.get();
    assertEquals("p1", m.getPartitionPath());
    assertEquals("file:///tmp/x.bin", m.getArtifactPath());
    assertEquals(10L, m.getEntryCount());
    assertEquals(1L, m.getMinKey());
    assertEquals(99L, m.getMaxKey());
    assertEquals("20240101120000", m.getBaseInstant());
    assertEquals("abc", m.getPartitionFingerprint());
    assertEquals(3, m.getFileCount());
    assertFalse(m.getIsDeleted());
  }

  @Test
  public void manifestRecordKeyMatchesSanitizeStem() {
    String partition = "year=2024/month=01";
    String recordKey = HoodieTableMetadataUtil.getRadixSplineManifestRecordKey(partition);
    HoodieRadixSplineIndexManifest m =
        HoodieRadixSplineIndexManifest.newBuilder()
            .setPartitionPath(partition)
            .setArtifactPath("s3://b/t.bin")
            .setEntryCount(1L)
            .setMinKey(0L)
            .setMaxKey(1L)
            .setBaseInstant("t")
            .setPartitionFingerprint("fp")
            .setFileCount(1)
            .setIsDeleted(false)
            .build();
    HoodieRecord<HoodieMetadataPayload> rec = HoodieMetadataPayload.createRadixSplineManifestRecord(recordKey, m);
    assertEquals(recordKey, rec.getRecordKey());
    assertEquals(MetadataPartitionType.RADIX_SPLINE_INDEX.getPartitionPath(), rec.getPartitionPath());
    assertTrue(rec.getData().getRadixSplineIndexMetadata().isPresent());
    assertEquals(m.getArtifactPath(), rec.getData().getRadixSplineIndexMetadata().get().getArtifactPath());
  }

  @Test
  public void radixSplineMetadataPartitionHasIndexVersion() {
    assertEquals(
        V1,
        HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.NINE, MetadataPartitionType.RADIX_SPLINE_INDEX));
  }

  @Test
  public void tableConfigIndicatesRadixSplineIndexFromProps() {
    HoodieTableConfig bloom = new HoodieTableConfig();
    bloom.getProps().setProperty(TABLE_INDEX_TYPE_PROP, "BLOOM");
    assertFalse(HoodieTableMetadataUtil.tableConfigIndicatesRadixSplineIndex(bloom));

    HoodieTableConfig radix = new HoodieTableConfig();
    radix.getProps().setProperty(TABLE_INDEX_TYPE_PROP, RADIX_SPLINE_INDEX_TYPE_NAME);
    assertTrue(HoodieTableMetadataUtil.tableConfigIndicatesRadixSplineIndex(radix));
  }
}
