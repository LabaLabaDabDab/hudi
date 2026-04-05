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

import org.apache.hudi.exception.HoodieIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

final class SimpleTempRadixArtifactWriter implements TempRadixArtifactWriter {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleTempRadixArtifactWriter.class);

  static final int MAGIC = 0x52534958; // RSIX
  static final int VERSION = 2;

  static final int HEADER_SIZE =
      Integer.BYTES + Integer.BYTES
          + Long.BYTES + Long.BYTES + Long.BYTES
          + Integer.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES
          + Long.BYTES + Long.BYTES + Long.BYTES + Long.BYTES
          + Long.BYTES + Long.BYTES + Long.BYTES;

  private static final int COPY_BUFFER_SIZE = 1024 * 1024;
  private static final int ARRAY_WRITE_BATCH_VALUES = 16 * 1024;

  private final RadixArtifactPublisher artifactPublisher;
  private final Path localTempDir;
  private final int maxEntriesPerPartition;

  SimpleTempRadixArtifactWriter(RadixArtifactPublisher artifactPublisher) {
    this(artifactPublisher, 0);
  }

  SimpleTempRadixArtifactWriter(RadixArtifactPublisher artifactPublisher, int maxEntriesPerPartition) {
    this(
        artifactPublisher,
        maxEntriesPerPartition,
        Paths.get(System.getProperty("java.io.tmpdir")));
  }

  /**
   * @param localTempDir directory for radix-entries/keys/offsets temp files (e.g. under table
   *     {@code .hoodie/.radix_index_tmp/.writer_scratch} for local file tables)
   */
  SimpleTempRadixArtifactWriter(
      RadixArtifactPublisher artifactPublisher,
      int maxEntriesPerPartition,
      Path localTempDir) {
    this.artifactPublisher = artifactPublisher;
    this.localTempDir = Objects.requireNonNull(localTempDir, "localTempDir");
    this.maxEntriesPerPartition = Math.max(0, maxEntriesPerPartition);
  }

  @Override
  public PartitionLookupDescriptor write(
      String partitionPath,
      String baseInstant,
      SortedRadixEntrySource sortedEntries,
      int maxError,
      int radixBits,
      String partitionFingerprint,
      int fileCount) throws IOException {

    long startedNanos = System.nanoTime();
    LOG.info(
        "RADIX writer start: partition={}, instant={}, maxError={}, radixBits={}, fileCount={}, fingerprint={}",
        partitionPath,
        baseInstant,
        maxError,
        radixBits,
        fileCount,
        shortFingerprint(partitionFingerprint));

    Path entriesFile = Files.createTempFile(localTempDir, "radix-entries-", ".bin");
    Path keysFile = Files.createTempFile(localTempDir, "radix-keys-", ".bin");
    Path offsetsFile = Files.createTempFile(localTempDir, "radix-offsets-", ".bin");
    Path localArtifact = null;

    long entryCount = 0;
    long minKey = Long.MAX_VALUE;
    long maxKey = Long.MIN_VALUE;
    RadixLocationEntry prev = null;

    try {
      long materializeStartedNanos = System.nanoTime();

      try (RandomAccessFile entriesRaf = new RandomAccessFile(entriesFile.toFile(), "rw");
          DataOutputStream keysOut =
              new DataOutputStream(
                  new BufferedOutputStream(Files.newOutputStream(keysFile), COPY_BUFFER_SIZE));
          DataOutputStream offsetsOut =
              new DataOutputStream(
                  new BufferedOutputStream(Files.newOutputStream(offsetsFile), COPY_BUFFER_SIZE))) {
        while (sortedEntries.hasNext()) {
          RadixLocationEntry entry = sortedEntries.next();

          if (maxEntriesPerPartition > 0 && entryCount >= maxEntriesPerPartition) {
            throw new HoodieIOException(
                "RADIX_SPLINE partition="
                    + partitionPath
                    + " exceeds hoodie.index.radix_spline.max_entries_per_partition="
                    + maxEntriesPerPartition
                    + " (entryCount="
                    + entryCount
                    + "). Increase the limit, reduce partition size, or use another index.");
          }

          if (prev != null) {
            if (entry.getEncodedKey() < prev.getEncodedKey()) {
              throw new IllegalArgumentException("sortedEntries must be sorted by encodedKey");
            }
            if (entry.getEncodedKey() == prev.getEncodedKey()) {
              throw new IllegalStateException(
                  "RADIX_SPLINE duplicate encodedKey detected in partition=" + partitionPath
                      + ", encodedKey=" + entry.getEncodedKey()
                      + ", prevRecordKey=" + prev.getRecordKey()
                      + ", currRecordKey=" + entry.getRecordKey());
            }
          }

          if (entryCount == Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "Too many keys to materialize for model build: " + (entryCount + 1));
          }

          long entryOffset = entriesRaf.getFilePointer();
          offsetsOut.writeLong(entryOffset);
          keysOut.writeLong(entry.getEncodedKey());
          writeEntry(entriesRaf, entry);

          minKey = Math.min(minKey, entry.getEncodedKey());
          maxKey = Math.max(maxKey, entry.getEncodedKey());
          entryCount++;
          prev = entry;
        }
      }

      long materializeElapsedMs = elapsedMs(materializeStartedNanos);
      long materializeKeysBytes = Files.size(keysFile);
      long materializeOffsetsBytes = Files.size(offsetsFile);
      long materializeEntriesBytes = Files.size(entriesFile);

      LOG.info(
          "RADIX writer entries materialized: partition={}, instant={}, entryCount={}, minKey={}, maxKey={}, "
              + "tempKeysBytes={}, tempOffsetsBytes={}, tempEntriesBytes={}, materializeMs={}",
          partitionPath,
          baseInstant,
          entryCount,
          entryCount == 0 ? null : minKey,
          entryCount == 0 ? null : maxKey,
          materializeKeysBytes,
          materializeOffsetsBytes,
          materializeEntriesBytes,
          materializeElapsedMs);

      if (entryCount == 0) {
        LOG.info(
            "RADIX writer empty partition: partition={}, instant={}, materializeMs={}",
            partitionPath,
            baseInstant,
            materializeElapsedMs);
        throw new HoodieRadixSplineIndex.EmptyPartitionBuildException(
            "Cannot build artifact for empty partition: " + partitionPath);
      }

      int n = (int) entryCount;

      long modelBuildStartedNanos = System.nanoTime();
      RadixSplineModel model;
      try (LongFileSortedKeyAccessor keyAccessor = new LongFileSortedKeyAccessor(keysFile, n)) {
        model = RadixSplineModel.build(keyAccessor, maxError, radixBits);
      }
      long modelBuildElapsedMs = elapsedMs(modelBuildStartedNanos);

      LOG.info(
          "RADIX spline model built: partition={}, instant={}, splineLen={}, radixLen={}, buildModelMs={}",
          partitionPath,
          baseInstant,
          model.splineKeys().length,
          model.radixMinIndex().length,
          modelBuildElapsedMs);

      localArtifact = Files.createTempFile(localTempDir, "radix-final-", ".bin");

      long assembleStartedNanos = System.nanoTime();
      long assembleSplineMs;
      long assembleKeysOffsetsMs;
      long assembleEntriesMs;
      long assembleHeaderMs;

      long splineKeysOffset;
      long splinePositionsOffset;
      long radixMinOffset;
      long radixMaxOffset;
      long keysOffset;
      long entryOffsetsOffset;
      long entriesOffset;

      long phaseNanos = System.nanoTime();
      try (CountingOutputStream countingOut =
               new CountingOutputStream(
                   new BufferedOutputStream(Files.newOutputStream(localArtifact), COPY_BUFFER_SIZE));
           DataOutputStream out = new DataOutputStream(countingOut)) {

        writeZeroBytes(out, HEADER_SIZE);

        splineKeysOffset = countingOut.getCount();
        writeLongArray(out, model.splineKeys());

        splinePositionsOffset = countingOut.getCount();
        writeIntArray(out, model.splinePositions());

        radixMinOffset = countingOut.getCount();
        writeIntArray(out, model.radixMinIndex());

        radixMaxOffset = countingOut.getCount();
        writeIntArray(out, model.radixMaxIndex());
        assembleSplineMs = elapsedMs(phaseNanos);

        phaseNanos = System.nanoTime();
        keysOffset = countingOut.getCount();
        writeLongArrayFromFile(out, keysFile, n);

        entryOffsetsOffset = countingOut.getCount();
        writeLongArrayFromFile(out, offsetsFile, n);
        assembleKeysOffsetsMs = elapsedMs(phaseNanos);

        phaseNanos = System.nanoTime();
        entriesOffset = countingOut.getCount();
        copyFile(entriesFile, out);
        assembleEntriesMs = elapsedMs(phaseNanos);

        out.flush();
      }

      phaseNanos = System.nanoTime();
      try (RandomAccessFile raf = new RandomAccessFile(localArtifact.toFile(), "rw")) {
        raf.seek(0L);
        writeHeader(
            raf,
            entryCount,
            minKey,
            maxKey,
            maxError,
            radixBits,
            model.splineKeys().length,
            model.radixMinIndex().length,
            splineKeysOffset,
            splinePositionsOffset,
            radixMinOffset,
            radixMaxOffset,
            keysOffset,
            entryOffsetsOffset,
            entriesOffset);
      }
      assembleHeaderMs = elapsedMs(phaseNanos);

      long assembleElapsedMs = elapsedMs(assembleStartedNanos);
      long localArtifactBytes = Files.size(localArtifact);

      LOG.info(
          "RADIX writer assemble phases: partition={}, instant={}, assembleSplineMs={}, "
              + "assembleKeysOffsetsMs={}, assembleEntriesMs={}, assembleHeaderMs={}, assembleTotalMs={}, "
              + "localArtifactBytes={}",
          partitionPath,
          baseInstant,
          assembleSplineMs,
          assembleKeysOffsetsMs,
          assembleEntriesMs,
          assembleHeaderMs,
          assembleElapsedMs,
          localArtifactBytes);

      long publishStartedNanos = System.nanoTime();
      String artifactPath = artifactPublisher.publish(partitionPath, baseInstant, localArtifact);
      long publishElapsedMs = elapsedMs(publishStartedNanos);

      long totalElapsedMs = elapsedMs(startedNanos);
      LOG.info(
          "RADIX writer phase totals: partition={}, instant={}, artifactPath={}, materializeMs={}, modelBuildMs={}, "
              + "assembleSplineMs={}, assembleKeysOffsetsMs={}, assembleEntriesMs={}, assembleHeaderMs={}, "
              + "assembleTotalMs={}, publishMs={}, writerTotalMs={}",
          partitionPath,
          baseInstant,
          artifactPath,
          materializeElapsedMs,
          modelBuildElapsedMs,
          assembleSplineMs,
          assembleKeysOffsetsMs,
          assembleEntriesMs,
          assembleHeaderMs,
          assembleElapsedMs,
          publishElapsedMs,
          totalElapsedMs);

      return new PartitionLookupDescriptor(
          partitionPath,
          artifactPath,
          entryCount,
          minKey,
          maxKey,
          baseInstant,
          partitionFingerprint,
          fileCount);

    } finally {
      long cleanupStartedNanos = System.nanoTime();

      deleteTempFileQuietly(entriesFile, "entriesFile", partitionPath, baseInstant);
      deleteTempFileQuietly(keysFile, "keysFile", partitionPath, baseInstant);
      deleteTempFileQuietly(offsetsFile, "offsetsFile", partitionPath, baseInstant);
      if (localArtifact != null) {
        deleteTempFileQuietly(localArtifact, "localArtifact", partitionPath, baseInstant);
      }

      LOG.info(
          "RADIX writer temp cleanup finished: partition={}, instant={}, cleanupMs={}, totalMs={}",
          partitionPath,
          baseInstant,
          elapsedMs(cleanupStartedNanos),
          elapsedMs(startedNanos));
    }
  }

  private static void writeHeader(
      RandomAccessFile out,
      long entryCount,
      long minKey,
      long maxKey,
      int maxError,
      int radixBits,
      int splineLen,
      int radixLen,
      long splineKeysOffset,
      long splinePositionsOffset,
      long radixMinOffset,
      long radixMaxOffset,
      long keysOffset,
      long entryOffsetsOffset,
      long entriesOffset) throws IOException {

    out.writeInt(MAGIC);
    out.writeInt(VERSION);
    out.writeLong(entryCount);
    out.writeLong(minKey);
    out.writeLong(maxKey);
    out.writeInt(maxError);
    out.writeInt(radixBits);
    out.writeInt(splineLen);
    out.writeInt(radixLen);
    out.writeLong(splineKeysOffset);
    out.writeLong(splinePositionsOffset);
    out.writeLong(radixMinOffset);
    out.writeLong(radixMaxOffset);
    out.writeLong(keysOffset);
    out.writeLong(entryOffsetsOffset);
    out.writeLong(entriesOffset);
  }

  private static void writeEntry(RandomAccessFile out, RadixLocationEntry entry) throws IOException {
    out.writeLong(entry.getEncodedKey());
    out.writeUTF(entry.getRecordKey());
    out.writeUTF(entry.getLocation().getInstantTime());
    out.writeUTF(entry.getLocation().getFileId());
  }

  private static void writeLongArrayFromFile(DataOutputStream out, Path file, int count)
      throws IOException {
    byte[] buffer = new byte[COPY_BUFFER_SIZE];
    try (BufferedInputStream bin = new BufferedInputStream(Files.newInputStream(file), COPY_BUFFER_SIZE);
        DataInputStream din = new DataInputStream(bin)) {
      int remaining = count;
      while (remaining > 0) {
        int batch = Math.min(remaining, ARRAY_WRITE_BATCH_VALUES);
        int byteLen = batch * Long.BYTES;
        din.readFully(buffer, 0, byteLen);
        out.write(buffer, 0, byteLen);
        remaining -= batch;
      }
    }
  }

  private static void writeLongArray(DataOutputStream out, long[] values) throws IOException {
    byte[] buffer = new byte[ARRAY_WRITE_BATCH_VALUES * Long.BYTES];
    int index = 0;
    while (index < values.length) {
      int batchSize = Math.min(ARRAY_WRITE_BATCH_VALUES, values.length - index);
      int pos = 0;
      for (int i = 0; i < batchSize; i++) {
        long v = values[index + i];
        buffer[pos++] = (byte) (v >>> 56);
        buffer[pos++] = (byte) (v >>> 48);
        buffer[pos++] = (byte) (v >>> 40);
        buffer[pos++] = (byte) (v >>> 32);
        buffer[pos++] = (byte) (v >>> 24);
        buffer[pos++] = (byte) (v >>> 16);
        buffer[pos++] = (byte) (v >>> 8);
        buffer[pos++] = (byte) v;
      }
      out.write(buffer, 0, pos);
      index += batchSize;
    }
  }

  private static void writeIntArray(DataOutputStream out, int[] values) throws IOException {
    byte[] buffer = new byte[ARRAY_WRITE_BATCH_VALUES * Integer.BYTES];
    int index = 0;
    while (index < values.length) {
      int batchSize = Math.min(ARRAY_WRITE_BATCH_VALUES, values.length - index);
      int pos = 0;
      for (int i = 0; i < batchSize; i++) {
        int v = values[index + i];
        buffer[pos++] = (byte) (v >>> 24);
        buffer[pos++] = (byte) (v >>> 16);
        buffer[pos++] = (byte) (v >>> 8);
        buffer[pos++] = (byte) v;
      }
      out.write(buffer, 0, pos);
      index += batchSize;
    }
  }

  private static void copyFile(Path source, DataOutputStream out) throws IOException {
    byte[] buffer = new byte[COPY_BUFFER_SIZE];
    try (BufferedInputStream in = new BufferedInputStream(Files.newInputStream(source), COPY_BUFFER_SIZE)) {
      int read;
      while ((read = in.read(buffer)) >= 0) {
        out.write(buffer, 0, read);
      }
    }
  }

  private static void writeZeroBytes(DataOutputStream out, int count) throws IOException {
    byte[] zeros = new byte[Math.min(count, 8192)];
    int remaining = count;
    while (remaining > 0) {
      int chunk = Math.min(remaining, zeros.length);
      out.write(zeros, 0, chunk);
      remaining -= chunk;
    }
  }

  private static void deleteTempFileQuietly(
      Path path,
      String label,
      String partitionPath,
      String baseInstant) throws IOException {
    if (path == null) {
      return;
    }

    boolean deleted = Files.deleteIfExists(path);
    LOG.debug(
        "RADIX temp file cleanup: partition={}, instant={}, label={}, path={}, deleted={}",
        partitionPath,
        baseInstant,
        label,
        path,
        deleted);
  }

  private static long elapsedMs(long startedNanos) {
    return (System.nanoTime() - startedNanos) / 1_000_000L;
  }

  private static String shortFingerprint(String fingerprint) {
    if (fingerprint == null || fingerprint.length() <= 12) {
      return fingerprint;
    }
    return fingerprint.substring(0, 12);
  }

  @Override
  public void close() throws IOException {
    artifactPublisher.close();
  }

  private static final class CountingOutputStream extends OutputStream {
    private final OutputStream delegate;
    private long count;

    private CountingOutputStream(OutputStream delegate) {
      this.delegate = delegate;
      this.count = 0L;
    }

    long getCount() {
      return count;
    }

    @Override
    public void write(int b) throws IOException {
      delegate.write(b);
      count++;
    }

    @Override
    public void write(byte[] b) throws IOException {
      delegate.write(b);
      count += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      delegate.write(b, off, len);
      count += len;
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}