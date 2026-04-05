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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

final class InMemoryRadixEntrySorter implements SpillableRadixEntrySorter {

  private final List<RadixLocationEntry> entries = new ArrayList<>();
  private boolean finished = false;

  @Override
  public void add(RadixLocationEntry entry) {
    if (finished) {
      throw new IllegalStateException("Sorter already finished");
    }
    entries.add(Objects.requireNonNull(entry, "entry must not be null"));
  }

  @Override
  public SortedRadixEntrySource finish() {
    if (finished) {
      throw new IllegalStateException("Sorter already finished");
    }
    finished = true;

    entries.sort(Comparator
        .comparingLong(RadixLocationEntry::getEncodedKey)
        .thenComparing(RadixLocationEntry::getRecordKey));

    return new ListSortedRadixEntrySource(entries);
  }

  @Override
  public void close() throws IOException {
    entries.clear();
  }

  private static final class ListSortedRadixEntrySource implements SortedRadixEntrySource {
    private final List<RadixLocationEntry> entries;
    private int index = 0;

    private ListSortedRadixEntrySource(List<RadixLocationEntry> entries) {
      this.entries = entries;
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
      // no-op
    }
  }
}