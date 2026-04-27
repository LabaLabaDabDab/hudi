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

import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

final class SimpleTempRadixArtifactReader implements TempRadixArtifactReader {

  private final SeekableDataInputStream input;
  private final Object streamLock;
  private final int entryCount;
  private final long[] entryOffsets;
  private final long entriesOffset;
  private final RadixSplineLookup lookup;
  private final int artifactVersion;
  private final String[] instantTimesById;
  private final String[] fileIdsById;

  private SimpleTempRadixArtifactReader(
      SeekableDataInputStream input,
      Object streamLock,
      int entryCount,
      long[] entryOffsets,
      long entriesOffset,
      RadixSplineLookup lookup,
      int artifactVersion,
      String[] instantTimesById,
      String[] fileIdsById) {
    this.input = input;
    this.streamLock = streamLock;
    this.entryCount = entryCount;
    this.entryOffsets = entryOffsets;
    this.entriesOffset = entriesOffset;
    this.lookup = lookup;
    this.artifactVersion = artifactVersion;
    this.instantTimesById = instantTimesById;
    this.fileIdsById = fileIdsById;
  }

  static SimpleTempRadixArtifactReader open(
      String artifactUri,
      StorageConfiguration<?> storageConf) throws IOException {
    return open(artifactUri, storageConf, RadixLookupWindowParams.fixed(4096));
  }

  static SimpleTempRadixArtifactReader open(
      String artifactUri,
      StorageConfiguration<?> storageConf,
      int lookupWindowKeys) throws IOException {
    return open(artifactUri, storageConf, RadixLookupWindowParams.fixed(lookupWindowKeys));
  }

  static SimpleTempRadixArtifactReader open(
      String artifactUri,
      StorageConfiguration<?> storageConf,
      RadixLookupWindowParams lookupWindowParams) throws IOException {

    StoragePath path = new StoragePath(artifactUri);
    HoodieStorage storage = HoodieStorageUtils.getStorage(path, storageConf);
    int bufferSize = storage.getDefaultBufferSize();
    SeekableDataInputStream in = storage.openSeekable(path, bufferSize, false);
    Object streamLock = new Object();

    try {
      int magic;
      int version;
      long entryCountLong;
      long minKey;
      long maxKey;
      int maxError;
      int radixBits;
      int splineLen;
      int radixLen;
      long splineKeysOffset;
      long splinePositionsOffset;
      long radixMinOffset;
      long radixMaxOffset;
      long keysOffset;
      long entryOffsetsOffset;
      long entriesOffset;

      synchronized (streamLock) {
        in.seek(0L);
        magic = in.readInt();
        version = in.readInt();
        entryCountLong = in.readLong();
        minKey = in.readLong();
        maxKey = in.readLong();
        maxError = in.readInt();
        radixBits = in.readInt();
        splineLen = in.readInt();
        radixLen = in.readInt();
        splineKeysOffset = in.readLong();
        splinePositionsOffset = in.readLong();
        radixMinOffset = in.readLong();
        radixMaxOffset = in.readLong();
        keysOffset = in.readLong();
        entryOffsetsOffset = in.readLong();
        entriesOffset = in.readLong();
      }

      if (magic != SimpleTempRadixArtifactWriter.MAGIC) {
        throw new IOException("Invalid artifact magic: " + magic);
      }
      if (version != SimpleTempRadixArtifactWriter.VERSION
          && version != SimpleTempRadixArtifactWriter.UTF8_VERSION
          && version != SimpleTempRadixArtifactWriter.LEGACY_VERSION) {
        throw new IOException("Unsupported artifact version: " + version);
      }

      int entryCount = Math.toIntExact(entryCountLong);

      RadixArtifactOpenScratch openScratch = new RadixArtifactOpenScratch();
      long[] splineKeys = openScratch.readLongArray(in, streamLock, splineKeysOffset, splineLen);
      int[] splinePositions = openScratch.readIntArray(in, streamLock, splinePositionsOffset, splineLen);
      int[] radixMinIndex = openScratch.readIntArray(in, streamLock, radixMinOffset, radixLen);
      int[] radixMaxIndex = openScratch.readIntArray(in, streamLock, radixMaxOffset, radixLen);

      RadixSplineModel model = RadixSplineModel.fromSerializedForm(
          entryCount,
          maxError,
          radixBits,
          minKey,
          maxKey,
          splineKeys,
          splinePositions,
          radixMinIndex,
          radixMaxIndex);

      SeekableWindowedKeyAccessor keyAccessor =
          new SeekableWindowedKeyAccessor(in, streamLock, keysOffset, entryCount, lookupWindowParams);
      RadixSplineLookup lookup = RadixSplineLookup.fromModel(keyAccessor, model);
      long[] entryOffsets = openScratch.readLongArray(in, streamLock, entryOffsetsOffset, entryCount);
      String[] instantTimesById = null;
      String[] fileIdsById = null;
      if (version >= 4) {
        synchronized (streamLock) {
          in.seek(entriesOffset);
          instantTimesById = readStringDictionary(in);
          fileIdsById = readStringDictionary(in);
        }
      }

      return new SimpleTempRadixArtifactReader(
          in,
          streamLock,
          entryCount,
          entryOffsets,
          entriesOffset,
          lookup,
          version,
          instantTimesById,
          fileIdsById);
    } catch (Throwable t) {
      try {
        in.close();
      } catch (IOException ignore) {
        // no-op
      }
      throw t;
    }
  }

  @Override
  public RadixSplineLookup getLookup() {
    return lookup;
  }

  @Override
  public RadixLocationEntry entryAt(int position) throws IOException {
    return entryAtWithTiming(position, null);
  }

  RadixLocationEntry entryAtIfEncodedKeyMatches(int position, long expectedEncodedKey) throws IOException {
    return entryAtIfEncodedKeyMatchesWithTiming(position, expectedEncodedKey, null);
  }

  RadixLocationEntry entryAtIfEncodedKeyMatchesWithTiming(
      int position,
      long expectedEncodedKey,
      EntryAtTiming timing) throws IOException {
    return readEntry(position, true, expectedEncodedKey, timing);
  }

  RadixLocationEntry entryAtWithTiming(int position, EntryAtTiming timing) throws IOException {
    return readEntry(position, false, 0L, timing);
  }

  private RadixLocationEntry readEntry(
      int position,
      boolean checkEncodedKey,
      long expectedEncodedKey,
      EntryAtTiming timing) throws IOException {
    if (position < 0 || position >= entryCount) {
      throw new IndexOutOfBoundsException(
          "position=" + position + ", entryCount=" + entryCount);
    }

    long t0 = System.nanoTime();
    long relativeEntryOffset = entryOffsets[position];
    long offsetLookupNs = System.nanoTime() - t0;

    synchronized (streamLock) {
      long t1 = System.nanoTime();
      input.seek(entriesOffset + relativeEntryOffset);
      long encodedKey = input.readLong();
      if (checkEncodedKey && encodedKey != expectedEncodedKey) {
        if (timing != null) {
          timing.record(offsetLookupNs, 0L);
        }
        return null;
      }
      String recordKey = readRecordKey();
      String instantTime;
      String fileId;
      if (artifactVersion >= 4) {
        int instantId = input.readInt();
        int fileIdId = input.readInt();
        if (instantTimesById == null || fileIdsById == null
            || instantId < 0 || instantId >= instantTimesById.length
            || fileIdId < 0 || fileIdId >= fileIdsById.length) {
          throw new IOException("Invalid dictionary id(s) in radix artifact entry: instantId="
              + instantId + ", fileIdId=" + fileIdId);
        }
        instantTime = instantTimesById[instantId];
        fileId = fileIdsById[fileIdId];
      } else {
        instantTime = readEntryString();
        fileId = readEntryString();
      }
      long payloadReadNs = System.nanoTime() - t1;
      if (timing != null) {
        timing.record(offsetLookupNs, payloadReadNs);
      }

      return new RadixLocationEntry(
          encodedKey,
          recordKey,
          new HoodieRecordLocation(instantTime, fileId));
    }
  }

  private String readRecordKey() throws IOException {
    if (artifactVersion == SimpleTempRadixArtifactWriter.LEGACY_VERSION) {
      return input.readUTF();
    }
    return readEntryString();
  }

  private String readEntryString() throws IOException {
    int len = input.readInt();
    if (len < 0) {
      throw new IOException("Invalid negative string length in radix artifact: " + len);
    }
    byte[] bytes = new byte[len];
    input.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static String[] readStringDictionary(SeekableDataInputStream in) throws IOException {
    int count = in.readInt();
    if (count < 0) {
      throw new IOException("Invalid negative dictionary size in radix artifact: " + count);
    }
    String[] values = new String[count];
    for (int i = 0; i < count; i++) {
      int len = in.readInt();
      if (len < 0) {
        throw new IOException("Invalid negative dictionary string length in radix artifact: " + len);
      }
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      values[i] = new String(bytes, StandardCharsets.UTF_8);
    }
    return values;
  }

  static final class EntryAtTiming {
    private long offsetLookupNs;
    private long payloadReadNs;
    private long payloadReadCalls;

    void record(long offsetLookupNs, long payloadReadNs) {
      this.offsetLookupNs += offsetLookupNs;
      this.payloadReadNs += payloadReadNs;
      this.payloadReadCalls++;
    }

    long getOffsetLookupNs() {
      return offsetLookupNs;
    }

    long getPayloadReadNs() {
      return payloadReadNs;
    }

    long getPayloadReadCalls() {
      return payloadReadCalls;
    }
  }

  @Override
  public int size() {
    return entryCount;
  }

  @Override
  public void close() throws IOException {
    input.close();
  }
}
