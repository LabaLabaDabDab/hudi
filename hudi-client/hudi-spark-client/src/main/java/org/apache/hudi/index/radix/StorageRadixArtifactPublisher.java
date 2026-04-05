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

import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.UUID;

/**
 * Publishes a locally materialized radix artifact into the table's {@link HoodieStorage}
 * (HDFS, S3, local FS, etc.) and returns a fully-qualified URI string for later opens.
 */
final class StorageRadixArtifactPublisher implements RadixArtifactPublisher {

  private static final int COPY_BUFFER_SIZE = 1024 * 1024;

  private final HoodieStorage storage;
  private final StoragePath artifactRoot;

  StorageRadixArtifactPublisher(HoodieStorage storage, StoragePath artifactRoot) {
    this.storage = storage;
    this.artifactRoot = artifactRoot;
  }

  @Override
  public String publish(
      String partitionPath,
      String baseInstant,
      Path localArtifact) throws IOException {

    StoragePath partitionDir = new StoragePath(
        new StoragePath(new StoragePath(artifactRoot, "instants"), baseInstant),
        sanitizePartition(partitionPath));

    storage.createDirectory(partitionDir);

    String fileName = "radix-" + UUID.randomUUID() + ".bin";
    StoragePath target = new StoragePath(partitionDir, fileName);

    try (InputStream in = Files.newInputStream(localArtifact);
         OutputStream out = storage.create(target, true)) {
      byte[] buffer = new byte[COPY_BUFFER_SIZE];
      int read;
      while ((read = in.read(buffer)) >= 0) {
        out.write(buffer, 0, read);
      }
    }

    StoragePath qualified = target.makeQualified(storage.getUri());
    return qualified.toString();
  }

  private static String sanitizePartition(String partitionPath) {
    if (partitionPath == null || partitionPath.isEmpty()) {
      return "__root__";
    }
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(partitionPath.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void close() {
    // storage lifecycle is owned by HoodieTable / meta client
  }
}
