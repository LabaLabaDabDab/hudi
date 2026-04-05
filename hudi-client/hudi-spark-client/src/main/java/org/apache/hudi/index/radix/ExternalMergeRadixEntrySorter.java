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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

final class ExternalMergeRadixEntrySorter implements SpillableRadixEntrySorter {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalMergeRadixEntrySorter.class);

  private static final Comparator<RadixLocationEntry> ENTRY_COMPARATOR =
      Comparator.comparingLong(RadixLocationEntry::getEncodedKey)
          .thenComparing(RadixLocationEntry::getRecordKey);

  private final Path tempDir;
  private final int maxEntriesInMemory;

  private final List<RadixLocationEntry> buffer;
  private final List<Path> spillFiles;

  private long mergeSpillBytesWritten;
  private int mergeSpillChunkCount;

  private boolean finished;

  ExternalMergeRadixEntrySorter(Path tempDir, int maxEntriesInMemory) {
    if (maxEntriesInMemory <= 0) {
      throw new IllegalArgumentException("maxEntriesInMemory must be > 0");
    }
    this.tempDir = Objects.requireNonNull(tempDir, "tempDir must not be null");
    this.maxEntriesInMemory = maxEntriesInMemory;
    this.buffer = new ArrayList<>(Math.min(maxEntriesInMemory, 16 * 1024));
    this.spillFiles = new ArrayList<>();
    this.mergeSpillBytesWritten = 0L;
    this.mergeSpillChunkCount = 0;
    this.finished = false;
  }

  @Override
  public long getMergeSpillBytesWritten() {
    return mergeSpillBytesWritten;
  }

  @Override
  public int getMergeSpillChunkCount() {
    return mergeSpillChunkCount;
  }

  @Override
  public void add(RadixLocationEntry entry) throws IOException {
    ensureNotFinished();
    buffer.add(Objects.requireNonNull(entry, "entry must not be null"));

    if (buffer.size() >= maxEntriesInMemory) {
      spillCurrentChunk();
    }
  }

  @Override
  public SortedRadixEntrySource finish() throws IOException {
    ensureNotFinished();
    finished = true;

    if (spillFiles.isEmpty()) {
      buffer.sort(ENTRY_COMPARATOR);
      return new InMemorySortedRadixEntrySource(new ArrayList<>(buffer));
    }

    if (!buffer.isEmpty()) {
      spillCurrentChunk();
    }

    return new MergedSortedRadixEntrySource(spillFiles);
  }

  private void spillCurrentChunk() throws IOException {
    if (buffer.isEmpty()) {
      return;
    }

    buffer.sort(ENTRY_COMPARATOR);

    Path spillFile = Files.createTempFile(tempDir, "radix-spill-", ".bin");
    try (DataOutputStream out =
             new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(spillFile)))) {
      for (RadixLocationEntry entry : buffer) {
        writeEntry(out, entry);
      }
    }

    long chunkBytes = Files.size(spillFile);
    mergeSpillBytesWritten += chunkBytes;
    mergeSpillChunkCount++;
    LOG.debug(
        "RADIX merge spill chunk written: path={}, chunkBytes={}, totalSpillBytes={}, chunkIndex={}",
        spillFile,
        chunkBytes,
        mergeSpillBytesWritten,
        mergeSpillChunkCount);

    spillFiles.add(spillFile);
    buffer.clear();
  }

  @Override
  public void close() throws IOException {
    IOException first = null;

    buffer.clear();

    for (Path spillFile : spillFiles) {
      try {
        Files.deleteIfExists(spillFile);
      } catch (IOException ioe) {
        if (first == null) {
          first = ioe;
        }
      }
    }
    spillFiles.clear();

    if (first != null) {
      throw first;
    }
  }

  private void ensureNotFinished() {
    if (finished) {
      throw new IllegalStateException("Sorter already finished");
    }
  }

  private static void writeEntry(DataOutputStream out, RadixLocationEntry entry) throws IOException {
    out.writeLong(entry.getEncodedKey());
    out.writeUTF(entry.getRecordKey());
    out.writeUTF(entry.getLocation().getInstantTime());
    out.writeUTF(entry.getLocation().getFileId());
  }

  private static RadixLocationEntry readEntry(DataInputStream in) throws IOException {
    try {
      long encodedKey = in.readLong();
      String recordKey = in.readUTF();
      String instantTime = in.readUTF();
      String fileId = in.readUTF();
      return new RadixLocationEntry(
          encodedKey,
          recordKey,
          new HoodieRecordLocation(instantTime, fileId));
    } catch (EOFException eof) {
      return null;
    }
  }

  private static final class InMemorySortedRadixEntrySource implements SortedRadixEntrySource {
    private final List<RadixLocationEntry> entries;
    private int index;

    private InMemorySortedRadixEntrySource(List<RadixLocationEntry> entries) {
      this.entries = entries;
      this.index = 0;
    }

    @Override
    public boolean hasNext() {
      return index < entries.size();
    }

    @Override
    public RadixLocationEntry next() {
      return entries.get(index++);
    }

    @Override
    public void close() {
      entries.clear();
    }
  }

  private static final class SpillFileReader implements Closeable {
    private final Path path;
    private final DataInputStream in;
    private RadixLocationEntry current;

    private SpillFileReader(Path path) throws IOException {
      this.path = path;
      this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
      this.current = readEntry(in);
    }

    boolean hasCurrent() {
      return current != null;
    }

    RadixLocationEntry current() {
      return current;
    }

    void advance() throws IOException {
      current = readEntry(in);
    }

    @Override
    public void close() throws IOException {
      try {
        in.close();
      } finally {
        Files.deleteIfExists(path);
      }
    }
  }

  private static final class HeapItem {
    private final SpillFileReader reader;
    private final RadixLocationEntry entry;

    private HeapItem(SpillFileReader reader, RadixLocationEntry entry) {
      this.reader = reader;
      this.entry = entry;
    }
  }

  private static final class MergedSortedRadixEntrySource implements SortedRadixEntrySource {

    private final List<SpillFileReader> readers;
    private final PriorityQueue<HeapItem> heap;

    private MergedSortedRadixEntrySource(List<Path> spillFiles) throws IOException {
      this.readers = new ArrayList<>(spillFiles.size());
      this.heap = new PriorityQueue<>(
          Comparator.<HeapItem>comparingLong(item -> item.entry.getEncodedKey())
              .thenComparing(item -> item.entry.getRecordKey()));

      try {
        for (Path spillFile : spillFiles) {
          SpillFileReader reader = new SpillFileReader(spillFile);
          readers.add(reader);
          if (reader.hasCurrent()) {
            heap.add(new HeapItem(reader, reader.current()));
          }
        }
      } catch (IOException ioe) {
        close();
        throw ioe;
      }
    }

    @Override
    public boolean hasNext() {
      return !heap.isEmpty();
    }

    @Override
    public RadixLocationEntry next() throws IOException {
      HeapItem item = heap.poll();
      if (item == null) {
        throw new IllegalStateException("No more entries");
      }

      RadixLocationEntry result = item.entry;
      item.reader.advance();
      if (item.reader.hasCurrent()) {
        heap.add(new HeapItem(item.reader, item.reader.current()));
      }
      return result;
    }

    @Override
    public void close() throws IOException {
      IOException first = null;
      heap.clear();

      for (SpillFileReader reader : readers) {
        try {
          reader.close();
        } catch (IOException ioe) {
          if (first == null) {
            first = ioe;
          }
        }
      }
      readers.clear();

      if (first != null) {
        throw first;
      }
    }
  }
}