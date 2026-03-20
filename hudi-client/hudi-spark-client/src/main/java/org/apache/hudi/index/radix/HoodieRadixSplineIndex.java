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
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.HoodieKeyLocationFetchHandle;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class HoodieRadixSplineIndex extends HoodieIndex<Object, Object> {

  private final int maxError;
  private final int radixBits;

  private volatile RadixSplineKeyEncoder keyEncoder;
  private volatile String recordKeyField;
  private volatile String recordKeyFieldTypeDescription;

  public HoodieRadixSplineIndex(HoodieWriteConfig config) {
    super(config);
    Objects.requireNonNull(config, "config must not be null");

    this.maxError = config.getRadixSplineIndexMaxError();
    this.radixBits = config.getRadixSplineIndexRadixBits();
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records,
      HoodieEngineContext context,
      HoodieTable hoodieTable) {

    long tagStart = System.nanoTime();
    System.out.println("### RADIX tagLocation: start");

    Objects.requireNonNull(records, "records must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(hoodieTable, "hoodieTable must not be null");

    ensureKeyEncoderInitialized();

    long loadLookupsStart = System.nanoTime();
    List<PartitionLookup> lookups = loadPartitionLookups(records, hoodieTable);
    System.out.println("### RADIX tagLocation: loadPartitionLookups done, partitions="
        + lookups.size()
        + ", tookSec=" + secondsSince(loadLookupsStart));

    if (lookups.isEmpty()) {
      System.out.println("### RADIX tagLocation: no lookups, returning input, totalSec="
          + secondsSince(tagStart));
      return records;
    }

    long mapStart = System.nanoTime();
    Map<String, PartitionLookup> lookupByPartition = new HashMap<>();
    for (PartitionLookup lookup : lookups) {
      lookupByPartition.put(lookup.partitionPath, lookup);
    }
    System.out.println("### RADIX tagLocation: lookupByPartition built, size="
        + lookupByPartition.size()
        + ", tookSec=" + secondsSince(mapStart));

    HoodieData<HoodieRecord<R>> tagged = records.map(record -> {
      String partitionPath = record.getPartitionPath();
      PartitionLookup partitionLookup = lookupByPartition.get(partitionPath);
      if (partitionLookup == null) {
        return record;
      }

      String recordKey = record.getRecordKey();
      long encodedKey = encodeRecordKeyOrThrow(recordKey, partitionPath, "tagLocation");

      LocationLookupResult result = partitionLookup.lookup.lookup(encodedKey);
      if (!result.isFound()) {
        return record;
      }

      int pos = result.getPosition();
      if (pos < 0 || pos >= partitionLookup.entries.size()) {
        return record;
      }

      RadixLocationEntry candidate = partitionLookup.entries.get(pos);
      if (!recordKey.equals(candidate.getRecordKey())) {
        return record;
      }

      record.unseal();
      record.setCurrentLocation(candidate.getLocation());
      record.seal();
      return record;
    });

    System.out.println("### RADIX tagLocation: map transformation created, totalSec="
        + secondsSince(tagStart));

    return tagged;
  }

  protected <R> List<PartitionLookup> loadPartitionLookups(
      HoodieData<HoodieRecord<R>> records,
      HoodieTable hoodieTable) {

    long start = System.nanoTime();
    System.out.println("### RADIX loadPartitionLookups: start");

    ensureKeyEncoderInitialized();

    long touchedPartitionsStart = System.nanoTime();
    Set<String> touchedPartitions = records
        .map(HoodieRecord::getPartitionPath)
        .distinct()
        .collectAsList()
        .stream()
        .collect(Collectors.toSet());

    System.out.println("### RADIX loadPartitionLookups: touchedPartitions collected, count="
        + touchedPartitions.size()
        + ", tookSec=" + secondsSince(touchedPartitionsStart));

    if (touchedPartitions.isEmpty()) {
      System.out.println("### RADIX loadPartitionLookups: empty touchedPartitions, totalSec="
          + secondsSince(start));
      return Collections.emptyList();
    }

    List<PartitionLookup> result = new ArrayList<>();

    for (String partitionPath : touchedPartitions) {
      long partitionStart = System.nanoTime();
      System.out.println("### RADIX loadPartitionLookups: partition=" + partitionPath + " start");

      long loadEntriesStart = System.nanoTime();
      List<RadixLocationEntry> entries = loadPartitionEntries(partitionPath, hoodieTable);
      System.out.println("### RADIX loadPartitionLookups: partition=" + partitionPath
          + ", entriesLoaded=" + entries.size()
          + ", loadEntriesSec=" + secondsSince(loadEntriesStart));

      if (entries.isEmpty()) {
        System.out.println("### RADIX loadPartitionLookups: partition=" + partitionPath
            + " empty, totalPartitionSec=" + secondsSince(partitionStart));
        continue;
      }

      long sortStart = System.nanoTime();
      entries.sort(
          Comparator.comparingLong(RadixLocationEntry::getEncodedKey)
              .thenComparing(RadixLocationEntry::getRecordKey)
      );
      System.out.println("### RADIX loadPartitionLookups: partition=" + partitionPath
          + ", sortSec=" + secondsSince(sortStart));

      validateNoDuplicateEncodedKeys(partitionPath, entries);

      long keysStart = System.nanoTime();
      long[] keys = new long[entries.size()];
      for (int i = 0; i < entries.size(); i++) {
        keys[i] = entries.get(i).getEncodedKey();
      }
      System.out.println("### RADIX loadPartitionLookups: partition=" + partitionPath
          + ", keysBuilt=" + keys.length
          + ", keysSec=" + secondsSince(keysStart));

      long buildSplineStart = System.nanoTime();
      RadixSplineLookup lookup = RadixSplineLookup.build(keys, maxError, radixBits);
      System.out.println("### RADIX loadPartitionLookups: partition=" + partitionPath
          + ", splineBuiltSec=" + secondsSince(buildSplineStart));

      result.add(new PartitionLookup(partitionPath, entries, lookup));

      System.out.println("### RADIX loadPartitionLookups: partition=" + partitionPath
          + " done, totalPartitionSec=" + secondsSince(partitionStart));
    }

    System.out.println("### RADIX loadPartitionLookups: done, resultPartitions="
        + result.size()
        + ", totalSec=" + secondsSince(start));

    return result;
  }

  protected List<RadixLocationEntry> loadPartitionEntries(
      String partitionPath,
      HoodieTable hoodieTable) {

    long start = System.nanoTime();
    System.out.println("### RADIX loadPartitionEntries: partition=" + partitionPath + " start");

    ensureKeyEncoderInitialized();

    List<RadixLocationEntry> entries = new ArrayList<>();

    long baseFilesStart = System.nanoTime();
    List<HoodieBaseFile> baseFiles = new ArrayList<>();
    hoodieTable.getBaseFileOnlyView()
        .getLatestBaseFiles(partitionPath)
        .forEach(baseFiles::add);

    System.out.println("### RADIX loadPartitionEntries: partition=" + partitionPath
        + ", baseFiles=" + baseFiles.size()
        + ", listBaseFilesSec=" + secondsSince(baseFilesStart));

    int totalRawLocations = 0;
    int totalWrongPartition = 0;

    for (HoodieBaseFile baseFile : baseFiles) {
      long baseFileStart = System.nanoTime();
      System.out.println("### RADIX loadPartitionEntries: partition=" + partitionPath
          + ", file=" + baseFile.getFileName()
          + " start");

      int fileRawLocations = 0;
      int fileAccepted = 0;
      int fileWrongPartition = 0;

      Pair<String, HoodieBaseFile> partitionBaseFilePair = Pair.of(partitionPath, baseFile);

      HoodieKeyLocationFetchHandle fetchHandle = new HoodieKeyLocationFetchHandle(
          config,
          hoodieTable,
          partitionBaseFilePair,
          Option.empty()
      );

      try (ClosableIterator<Pair<HoodieKey, HoodieRecordLocation>> it = fetchHandle.locations()) {
        while (it.hasNext()) {
          fileRawLocations++;
          totalRawLocations++;

          Pair<HoodieKey, HoodieRecordLocation> entry = it.next();
          HoodieKey hoodieKey = entry.getLeft();

          if (!partitionPath.equals(hoodieKey.getPartitionPath())) {
            fileWrongPartition++;
            totalWrongPartition++;
            continue;
          }

          String recordKey = hoodieKey.getRecordKey();
          long encodedKey = encodeRecordKeyOrThrow(recordKey, partitionPath, "loadPartitionEntries");
          entries.add(new RadixLocationEntry(encodedKey, recordKey, entry.getRight()));
          fileAccepted++;
        }
      }

      System.out.println("### RADIX loadPartitionEntries: partition=" + partitionPath
          + ", file=" + baseFile.getFileName()
          + ", rawLocations=" + fileRawLocations
          + ", accepted=" + fileAccepted
          + ", wrongPartition=" + fileWrongPartition
          + ", unsupported=0"
          + ", fileSec=" + secondsSince(baseFileStart));
    }

    System.out.println("### RADIX loadPartitionEntries: partition=" + partitionPath
        + ", totalEntries=" + entries.size()
        + ", totalRawLocations=" + totalRawLocations
        + ", totalWrongPartition=" + totalWrongPartition
        + ", totalUnsupportedKeys=0"
        + ", totalSec=" + secondsSince(start));

    return entries;
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

  private void validateNoDuplicateEncodedKeys(String partitionPath, List<RadixLocationEntry> entries) {
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
    return true;
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

  static final class PartitionLookup implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String partitionPath;
    private final List<RadixLocationEntry> entries;
    private final RadixSplineLookup lookup;

    private PartitionLookup(String partitionPath,
                            List<RadixLocationEntry> entries,
                            RadixSplineLookup lookup) {
      this.partitionPath = partitionPath;
      this.entries = entries;
      this.lookup = lookup;
    }
  }

  private static String secondsSince(long startNanos) {
    return String.format("%.3f", (System.nanoTime() - startNanos) / 1_000_000_000.0d);
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
}