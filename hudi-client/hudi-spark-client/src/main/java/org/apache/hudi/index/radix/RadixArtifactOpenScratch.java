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

import org.apache.hudi.io.SeekableDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Reusable byte buffers and {@link ByteBuffer} views for reading primitive arrays while
 * opening a radix artifact (avoids per-array {@link ByteBuffer#wrap(byte[])} allocation).
 */
final class RadixArtifactOpenScratch {

  private byte[] longBytes;
  private ByteBuffer longBb;
  private byte[] intBytes;
  private ByteBuffer intBb;

  long[] readLongArray(
      SeekableDataInputStream in,
      Object streamLock,
      long offset,
      int size) throws IOException {
    long[] result = new long[size];
    if (size == 0) {
      return result;
    }
    int nbytes = Math.multiplyExact(size, Long.BYTES);
    ensureLongCapacity(nbytes);
    synchronized (streamLock) {
      in.seek(offset);
      in.readFully(longBytes, 0, nbytes);
    }
    longBb.clear();
    longBb.limit(nbytes);
    for (int i = 0; i < size; i++) {
      result[i] = longBb.getLong();
    }
    return result;
  }

  int[] readIntArray(
      SeekableDataInputStream in,
      Object streamLock,
      long offset,
      int size) throws IOException {
    int[] result = new int[size];
    if (size == 0) {
      return result;
    }
    int nbytes = Math.multiplyExact(size, Integer.BYTES);
    ensureIntCapacity(nbytes);
    synchronized (streamLock) {
      in.seek(offset);
      in.readFully(intBytes, 0, nbytes);
    }
    intBb.clear();
    intBb.limit(nbytes);
    for (int i = 0; i < size; i++) {
      result[i] = intBb.getInt();
    }
    return result;
  }

  private void ensureLongCapacity(int nbytes) {
    if (longBytes == null || longBytes.length < nbytes) {
      longBytes = new byte[nbytes];
      longBb = ByteBuffer.wrap(longBytes).order(ByteOrder.BIG_ENDIAN);
    }
  }

  private void ensureIntCapacity(int nbytes) {
    if (intBytes == null || intBytes.length < nbytes) {
      intBytes = new byte[nbytes];
      intBb = ByteBuffer.wrap(intBytes).order(ByteOrder.BIG_ENDIAN);
    }
  }
}
