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

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestUtils.createSimpleRecord;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieRadixSplineIndex extends HoodieSparkClientTestHarness {

  private static final Schema SCHEMA =
      getSchemaFromResource(TestHoodieRadixSplineIndex.class, "/exampleSchema.avsc", true);

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initHoodieStorage();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void testTagLocationFindsExistingBaseFileRecords() throws Exception {
    HoodieRecord<IndexedRecord> existing1 =
        createSimpleRecord("10", "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord<IndexedRecord> existing2 =
        createSimpleRecord("20", "2016-01-31T03:20:41.415Z", 100);
    HoodieRecord<IndexedRecord> missing =
        createSimpleRecord("30", "2016-01-31T03:25:41.415Z", 200);

    HoodieWriteConfig config = makeConfig();
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);

    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(table, SCHEMA);
    testTable.addCommit("001")
        .withInserts("2016/01/31", "file-1", existing1, existing2);

    JavaRDD<HoodieRecord<IndexedRecord>> inputRdd =
        jsc.parallelize(Arrays.asList(existing1, existing2, missing));

    HoodieData<HoodieRecord<IndexedRecord>> input = HoodieJavaRDD.of(inputRdd);

    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(config);
    HoodieData<HoodieRecord<IndexedRecord>> tagged =
        index.tagLocation(input, context, HoodieSparkTable.create(config, context, metaClient));

    assertEquals(3, tagged.collectAsList().size());

    HoodieRecord<IndexedRecord> tagged10 = findByRecordKey(tagged, "10");
    HoodieRecord<IndexedRecord> tagged20 = findByRecordKey(tagged, "20");
    HoodieRecord<IndexedRecord> tagged30 = findByRecordKey(tagged, "30");

    assertTrue(tagged10.isCurrentLocationKnown());
    assertTrue(tagged20.isCurrentLocationKnown());
    assertFalse(tagged30.isCurrentLocationKnown());

    assertEquals("001", tagged10.getCurrentLocation().getInstantTime());
    assertEquals("001", tagged20.getCurrentLocation().getInstantTime());
  }

  @Test
  public void testTagLocationWithOnlyMissingRecords() throws Exception {
    HoodieRecord<IndexedRecord> missing1 =
        createSimpleRecord("100", "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord<IndexedRecord> missing2 =
        createSimpleRecord("200", "2016-01-31T03:20:41.415Z", 100);

    HoodieWriteConfig config = makeConfig();
    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(config);

    JavaRDD<HoodieRecord<IndexedRecord>> inputRdd =
        jsc.parallelize(Arrays.asList(missing1, missing2));
    HoodieData<HoodieRecord<IndexedRecord>> input = HoodieJavaRDD.of(inputRdd);

    HoodieData<HoodieRecord<IndexedRecord>> tagged =
        index.tagLocation(input, context, HoodieSparkTable.create(config, context, metaClient));

    assertEquals(2, tagged.collectAsList().size());
    assertFalse(findByRecordKey(tagged, "100").isCurrentLocationKnown());
    assertFalse(findByRecordKey(tagged, "200").isCurrentLocationKnown());
  }

  @Test
  public void testTagLocationAcrossPartitions() throws Exception {
    HoodieRecord<IndexedRecord> partition1Existing =
        createSimpleRecord("10", "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord<IndexedRecord> partition2Existing =
        createSimpleRecord("11", "2015-01-31T03:16:41.415Z", 13);
    HoodieRecord<IndexedRecord> partition2Missing =
        createSimpleRecord("12", "2015-01-31T03:17:41.415Z", 14);

    HoodieWriteConfig config = makeConfig();
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);

    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(table, SCHEMA);
    testTable.addCommit("001")
        .withInserts("2016/01/31", "file-a", partition1Existing)
        .withInserts("2015/01/31", "file-b", partition2Existing);

    JavaRDD<HoodieRecord<IndexedRecord>> inputRdd =
        jsc.parallelize(Arrays.asList(partition1Existing, partition2Existing, partition2Missing));
    HoodieData<HoodieRecord<IndexedRecord>> input = HoodieJavaRDD.of(inputRdd);

    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(config);
    HoodieData<HoodieRecord<IndexedRecord>> tagged =
        index.tagLocation(input, context, HoodieSparkTable.create(config, context, metaClient));

    HoodieRecord<IndexedRecord> tagged10 = findByRecordKey(tagged, "10");
    HoodieRecord<IndexedRecord> tagged11 = findByRecordKey(tagged, "11");
    HoodieRecord<IndexedRecord> tagged12 = findByRecordKey(tagged, "12");

    assertTrue(tagged10.isCurrentLocationKnown());
    assertTrue(tagged11.isCurrentLocationKnown());
    assertFalse(tagged12.isCurrentLocationKnown());
  }

  @Test
  public void testTagLocationFailsForUnsupportedLogicalTypeRecordKeyField() throws Exception {
    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema customSchema = recordSchema("id", dateSchema);

    HoodieWriteConfig config = makeConfig(customSchema, "id");
    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(config);

    HoodieData<HoodieRecord<IndexedRecord>> emptyInput =
        HoodieJavaRDD.of(jsc.parallelize(Collections.emptyList()));

    IllegalArgumentException e = assertThrows(
        IllegalArgumentException.class,
        () -> index.tagLocation(emptyInput, context, HoodieSparkTable.create(config, context, metaClient)));

    assertTrue(e.getMessage().contains("logical type"));
    assertTrue(e.getMessage().contains("recordkey.field='id'"));
  }

  @Test
  public void testTagLocationWithNestedRecordKeyField() throws Exception {
    Schema nested = Schema.createRecord("Nested", null, "org.apache.hudi.index.radix", false);
    nested.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.LONG), null, (Object) null)
    ));

    Schema root = Schema.createRecord("Root", null, "org.apache.hudi.index.radix", false);
    root.setFields(Arrays.asList(
        new Schema.Field("a", nested, null, (Object) null)
    ));

    HoodieWriteConfig config = makeConfig(root, "a.id");
    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(config);

    HoodieData<HoodieRecord<IndexedRecord>> emptyInput =
        HoodieJavaRDD.of(jsc.parallelize(Collections.emptyList()));

    HoodieData<HoodieRecord<IndexedRecord>> tagged =
        index.tagLocation(emptyInput, context, HoodieSparkTable.create(config, context, metaClient));

    assertEquals(0, tagged.collectAsList().size());
  }

  @Test
  public void testTagLocationSmallStableBenchmarkScenario() throws Exception {
    HoodieWriteConfig config = makeConfig();
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);

    List<HoodieRecord<IndexedRecord>> existing = new ArrayList<>();
    List<HoodieRecord<IndexedRecord>> missing = new ArrayList<>();

    for (int i = 1; i <= 150; i++) {
      existing.add(createSimpleRecord(
          Integer.toString(i),
          "2016-01-31T03:16:41.415Z",
          i));
    }

    for (int i = 151; i <= 200; i++) {
      missing.add(createSimpleRecord(
          Integer.toString(i),
          "2016-01-31T03:16:41.415Z",
          i));
    }

    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(table, SCHEMA);
    testTable.addCommit("001")
        .withInserts("2016/01/31", "file-bench", existing.toArray(new HoodieRecord[0]));

    List<HoodieRecord<IndexedRecord>> inputRecords = new ArrayList<>();
    inputRecords.addAll(existing);
    inputRecords.addAll(missing);

    HoodieData<HoodieRecord<IndexedRecord>> input =
        HoodieJavaRDD.of(jsc.parallelize(inputRecords, 2));

    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(config);
    HoodieData<HoodieRecord<IndexedRecord>> tagged =
        index.tagLocation(input, context, HoodieSparkTable.create(config, context, metaClient));

    List<HoodieRecord<IndexedRecord>> out = tagged.collectAsList();
    assertEquals(200, out.size());

    long known = out.stream().filter(HoodieRecord::isCurrentLocationKnown).count();
    long unknown = out.stream().filter(r -> !r.isCurrentLocationKnown()).count();

    assertEquals(150L, known);
    assertEquals(50L, unknown);
  }

  private HoodieWriteConfig makeConfig() {
    return makeConfig(SCHEMA, "_row_key");
  }

  private HoodieWriteConfig makeConfig(Schema schema, String recordKeyField) {
    Properties props = new Properties();
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), recordKeyField);

    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(schema.toString())
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.RADIX_SPLINE)
            .build())
        .build();
  }

  private Schema recordSchema(String fieldName, Schema fieldSchema) {
    Schema record = Schema.createRecord("TestRecord", null, "org.apache.hudi.index.radix", false);
    record.setFields(Arrays.asList(
        new Schema.Field(fieldName, fieldSchema, null, (Object) null)
    ));
    return record;
  }

  private HoodieRecord<IndexedRecord> findByRecordKey(
      HoodieData<HoodieRecord<IndexedRecord>> records,
      String recordKey) {
    return records.collectAsList().stream()
        .filter(r -> r.getRecordKey().equals(recordKey))
        .findFirst()
        .orElseThrow(() -> new AssertionError("record not found: " + recordKey));
  }
}