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

import java.io.Closeable;
import java.io.IOException;

interface SpillableRadixEntrySorter extends Closeable {

  void add(RadixLocationEntry entry) throws IOException;

  SortedRadixEntrySource finish() throws IOException;

  /** Bytes written to on-disk spill files (0 if fully in-memory). */
  default long getMergeSpillBytesWritten() {
    return 0L;
  }

  /** Number of spill chunk files created (0 if fully in-memory). */
  default int getMergeSpillChunkCount() {
    return 0;
  }
}