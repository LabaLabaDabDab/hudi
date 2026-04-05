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

final class SimpleTempRadixArtifactReader implements TempRadixArtifactReader {

  private final SeekableDataInputStream input;
  private final Object streamLock;
  private final int entryCount;
  private final long entryOffsetsOffset;
  private final long entriesOffset;
  private final RadixSplineLookup lookup;

  private SimpleTempRadixArtifactReader(
      SeekableDataInputStream input,
      Object streamLock,
      int entryCount,
      long entryOffsetsOffset,
      long entriesOffset,
      RadixSplineLookup lookup) {
    this.input = input;
    this.streamLock = streamLock;
    this.entryCount = entryCount;
    this.entryOffsetsOffset = entryOffsetsOffset;
    this.entriesOffset = entriesOffset;
    this.lookup = lookup;
  }

  static SimpleTempRadixArtifactReader open(
      String artifactUri,
      StorageConfiguration<?> storageConf) throws IOException {

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
      if (version != SimpleTempRadixArtifactWriter.VERSION) {
        throw new IOException("Unsupported artifact version: " + version);
      }

      int entryCount = Math.toIntExact(entryCountLong);

      long[] splineKeys = readLongArray(in, streamLock, splineKeysOffset, splineLen);
      int[] splinePositions = readIntArray(in, streamLock, splinePositionsOffset, splineLen);
      int[] radixMinIndex = readIntArray(in, streamLock, radixMinOffset, radixLen);
      int[] radixMaxIndex = readIntArray(in, streamLock, radixMaxOffset, radixLen);

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

      SeekableStreamKeyAccessor keyAccessor =
          new SeekableStreamKeyAccessor(in, streamLock, keysOffset, entryCount);
      RadixSplineLookup lookup = RadixSplineLookup.fromModel(keyAccessor, model);

      return new SimpleTempRadixArtifactReader(
          in,
          streamLock,
          entryCount,
          entryOffsetsOffset,
          entriesOffset,
          lookup);
    } catch (Throwable t) {
      try {
        in.close();
      } catch (IOException ignore) {
        // no-op
      }
      throw t;
    }
  }

  private static long[] readLongArray(
      SeekableDataInputStream in,
      Object streamLock,
      long offset,
      int size) throws IOException {
    long[] result = new long[size];
    synchronized (streamLock) {
      in.seek(offset);
      for (int i = 0; i < size; i++) {
        result[i] = in.readLong();
      }
    }
    return result;
  }

  private static int[] readIntArray(
      SeekableDataInputStream in,
      Object streamLock,
      long offset,
      int size) throws IOException {
    int[] result = new int[size];
    synchronized (streamLock) {
      in.seek(offset);
      for (int i = 0; i < size; i++) {
        result[i] = in.readInt();
      }
    }
    return result;
  }

  @Override
  public RadixSplineLookup getLookup() {
    return lookup;
  }

  @Override
  public RadixLocationEntry entryAt(int position) throws IOException {
    if (position < 0 || position >= entryCount) {
      throw new IndexOutOfBoundsException(
          "position=" + position + ", entryCount=" + entryCount);
    }

    synchronized (streamLock) {
      input.seek(entryOffsetsOffset + ((long) position * Long.BYTES));
      long relativeEntryOffset = input.readLong();

      input.seek(entriesOffset + relativeEntryOffset);
      long encodedKey = input.readLong();
      String recordKey = input.readUTF();
      String instantTime = input.readUTF();
      String fileId = input.readUTF();

      return new RadixLocationEntry(
          encodedKey,
          recordKey,
          new HoodieRecordLocation(instantTime, fileId));
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
