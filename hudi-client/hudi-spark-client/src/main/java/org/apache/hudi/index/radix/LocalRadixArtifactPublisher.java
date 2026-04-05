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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;

/**
 * Local-filesystem publisher (e.g. tests). Production path uses {@link StorageRadixArtifactPublisher}.
 */
final class LocalRadixArtifactPublisher implements RadixArtifactPublisher {

  private final Path artifactRoot;

  LocalRadixArtifactPublisher(Path artifactRoot) {
    this.artifactRoot = artifactRoot;
  }

  @Override
  public String publish(
      String partitionPath,
      String baseInstant,
      Path localArtifact) throws IOException {

    Path partitionDir = artifactRoot
        .resolve("instants")
        .resolve(baseInstant)
        .resolve(sanitizePartition(partitionPath));

    Files.createDirectories(partitionDir);

    Path target = Files.createTempFile(partitionDir, "radix-", ".bin");
    Files.copy(localArtifact, target, StandardCopyOption.REPLACE_EXISTING);

    return target.toUri().toString();
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
    // no-op
  }
}
