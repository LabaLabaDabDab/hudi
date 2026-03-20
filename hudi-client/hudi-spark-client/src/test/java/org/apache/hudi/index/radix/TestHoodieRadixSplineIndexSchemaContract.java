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
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieRadixSplineIndexSchemaContract {

  @Test
  public void testRecordKeyFieldTypeIntIsAccepted() throws Exception {
    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(
        config("id", recordSchema("id", Schema.create(Schema.Type.INT))));
    invokeEnsureKeyEncoderInitialized(index);
  }

  @Test
  public void testRecordKeyFieldTypeLongIsAccepted() throws Exception {
    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(
        config("id", recordSchema("id", Schema.create(Schema.Type.LONG))));
    invokeEnsureKeyEncoderInitialized(index);
  }

  @Test
  public void testRecordKeyFieldTypeStringIsAccepted() throws Exception {
    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(
        config("id", recordSchema("id", Schema.create(Schema.Type.STRING))));
    invokeEnsureKeyEncoderInitialized(index);
  }

  @Test
  public void testDateLogicalTypeIsRejected() {
    Schema dateInt = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(
        config("id", recordSchema("id", dateInt)));

    IllegalArgumentException e = assertThrows(
        IllegalArgumentException.class,
        () -> invokeEnsureKeyEncoderInitialized(index));

    assertTrue(e.getMessage().contains("logical type"));
    assertTrue(e.getMessage().contains("id"));
  }

  @Test
  public void testTimestampMillisLogicalTypeIsRejected() {
    Schema tsLong = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(
        config("id", recordSchema("id", tsLong)));

    IllegalArgumentException e = assertThrows(
        IllegalArgumentException.class,
        () -> invokeEnsureKeyEncoderInitialized(index));

    assertTrue(e.getMessage().contains("logical type"));
    assertTrue(e.getMessage().contains("timestamp-millis"));
  }

  @Test
  public void testNestedFieldIsResolved() throws Exception {
    Schema nested = Schema.createRecord("Nested", null, "org.apache.hudi.index.radix", false);
    nested.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.LONG), null, (Object) null)
    ));

    Schema root = Schema.createRecord("Root", null, "org.apache.hudi.index.radix", false);
    root.setFields(Arrays.asList(
        new Schema.Field("a", nested, null, (Object) null)
    ));

    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(config("a.id", root));
    invokeEnsureKeyEncoderInitialized(index);
  }

  @Test
  public void testComplexUnionIsRejected() {
    Schema union = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.STRING)
    ));

    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(
        config("id", recordSchema("id", union)));

    IllegalArgumentException e = assertThrows(
        IllegalArgumentException.class,
        () -> invokeEnsureKeyEncoderInitialized(index));

    assertTrue(e.getMessage().contains("complex union"));
  }

  @Test
  public void testDuplicateEncodedKeyIsRejected() throws Exception {
    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(
        config("id", recordSchema("id", Schema.create(Schema.Type.STRING))));
    invokeEnsureKeyEncoderInitialized(index);

    List<RadixLocationEntry> entries = Arrays.asList(
        new RadixLocationEntry(123L, "123", new HoodieRecordLocation("001", "file-1")),
        new RadixLocationEntry(123L, "123", new HoodieRecordLocation("001", "file-2"))
    );

    IllegalStateException e = assertThrows(
        IllegalStateException.class,
        () -> invokeValidateNoDuplicateEncodedKeys(index, "p1", entries));

    assertTrue(e.getMessage().contains("duplicate encodedKey"));
    assertTrue(e.getMessage().contains("partition=p1"));
  }

  @Test
  public void testNonCanonicalStringRecordKeyFailsAtTaggingContractLevel() throws Exception {
    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(
        config("id", recordSchema("id", Schema.create(Schema.Type.STRING))));
    invokeEnsureKeyEncoderInitialized(index);

    IllegalArgumentException e = assertThrows(
        IllegalArgumentException.class,
        () -> invokeEncodeRecordKeyOrThrow(index, "00123", "p1", "unit-test"));

    assertTrue(e.getMessage().contains("leading zeros"));
    assertTrue(e.getMessage().contains("recordkey.field='id'"));
  }

  @Test
  public void testLeadingPlusStringRecordKeyFailsAtTaggingContractLevel() throws Exception {
    HoodieRadixSplineIndex index = new HoodieRadixSplineIndex(
        config("id", recordSchema("id", Schema.create(Schema.Type.STRING))));
    invokeEnsureKeyEncoderInitialized(index);

    IllegalArgumentException e = assertThrows(
        IllegalArgumentException.class,
        () -> invokeEncodeRecordKeyOrThrow(index, "+123", "p1", "unit-test"));

    assertTrue(e.getMessage().contains("leading '+'"));
  }

  private static HoodieWriteConfig config(String recordKeyField, Schema schema) {
    return HoodieWriteConfig.newBuilder()
        .withPath("file:///tmp/hudi-radix-schema-contract")
        .forTable("radix_schema_contract_test")
        .withSchema(schema.toString())
        .withProps(java.util.Collections.singletonMap(
            KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), recordKeyField))
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(org.apache.hudi.index.HoodieIndex.IndexType.RADIX_SPLINE)
            .build())
        .build();
  }

  private static Schema recordSchema(String fieldName, Schema fieldSchema) {
    Schema record = Schema.createRecord("TestRecord", null, "org.apache.hudi.index.radix", false);
    record.setFields(Arrays.asList(
        new Schema.Field(fieldName, fieldSchema, null, (Object) null)
    ));
    return record;
  }

  private static void invokeEnsureKeyEncoderInitialized(HoodieRadixSplineIndex index) throws Exception {
    Method m = HoodieRadixSplineIndex.class.getDeclaredMethod("ensureKeyEncoderInitialized");
    m.setAccessible(true);
    invoke(index, m);
  }

  private static void invokeValidateNoDuplicateEncodedKeys(
      HoodieRadixSplineIndex index, String partition, List<RadixLocationEntry> entries) throws Exception {
    Method m = HoodieRadixSplineIndex.class.getDeclaredMethod(
        "validateNoDuplicateEncodedKeys", String.class, List.class);
    m.setAccessible(true);
    invoke(index, m, partition, entries);
  }

  private static long invokeEncodeRecordKeyOrThrow(
      HoodieRadixSplineIndex index, String recordKey, String partition, String stage) throws Exception {
    Method m = HoodieRadixSplineIndex.class.getDeclaredMethod(
        "encodeRecordKeyOrThrow", String.class, String.class, String.class);
    m.setAccessible(true);
    return (Long) invoke(index, m, recordKey, partition, stage);
  }

  private static Object invoke(Object target, Method method, Object... args) throws Exception {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw e;
    }
  }
}