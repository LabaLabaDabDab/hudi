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

import java.io.Serializable;
import java.util.Objects;

final class PartitionLookupDescriptor implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String partitionPath;
  private final String artifactPath;
  private final long entryCount;
  private final long minKey;
  private final long maxKey;
  private final String baseInstant;
  private final String partitionFingerprint;
  private final int fileCount;

  PartitionLookupDescriptor(
      String partitionPath,
      String artifactPath,
      long entryCount,
      long minKey,
      long maxKey,
      String baseInstant, String partitionFingerprint, int fileCount) {
    this.partitionPath = Objects.requireNonNull(partitionPath, "partitionPath must not be null");
    this.artifactPath = Objects.requireNonNull(artifactPath, "artifactPath must not be null");
    this.entryCount = entryCount;
    this.minKey = minKey;
    this.maxKey = maxKey;
    this.baseInstant = Objects.requireNonNull(baseInstant, "baseInstant must not be null");
    this.partitionFingerprint = partitionFingerprint;
    this.fileCount = fileCount;
  }

  String getPartitionPath() {
    return partitionPath;
  }

  String getArtifactPath() {
    return artifactPath;
  }

  long getEntryCount() {
    return entryCount;
  }

  long getMinKey() {
    return minKey;
  }

  long getMaxKey() {
    return maxKey;
  }

  String getBaseInstant() {
    return baseInstant;
  }

  String getPartitionFingerprint() {
    return partitionFingerprint;
  }

  int getFileCount() {
    return fileCount;
  }

  @Override
  public String toString() {
    return "PartitionLookupDescriptor{"
        + "partitionPath='" + partitionPath + '\''
        + ", artifactPath='" + artifactPath + '\''
        + ", entryCount=" + entryCount
        + ", minKey=" + minKey
        + ", maxKey=" + maxKey
        + ", baseInstant='" + baseInstant + '\''
        + ", partitionFingerprint='" + partitionFingerprint + '\''
        + ", fileCount='" + fileCount + '\''
        + '}';
  }
}