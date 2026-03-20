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

import java.util.Objects;

/**
 * {@link SortedKeyAccessor} backed by an in-memory {@code long[]}.
 */
final class InMemoryKeyAccessor implements SortedKeyAccessor {

  private static final long serialVersionUID = 1L;

  private final long[] sortedKeys;

  InMemoryKeyAccessor(long[] sortedKeys) {
    this.sortedKeys = Objects.requireNonNull(sortedKeys, "sortedKeys must not be null");
  }

  @Override
  public int size() {
    return sortedKeys.length;
  }

  @Override
  public long keyAt(int index) {
    return sortedKeys[index];
  }
}
