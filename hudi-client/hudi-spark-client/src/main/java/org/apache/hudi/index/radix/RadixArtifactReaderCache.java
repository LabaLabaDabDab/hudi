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

import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class RadixArtifactReaderCache {

  private static final Logger LOG = LoggerFactory.getLogger(RadixArtifactReaderCache.class);

  private static final long IDLE_TTL_MS = 5 * 60 * 1000L;

  private static final ConcurrentHashMap<String, ReaderHolder> CACHE = new ConcurrentHashMap<>();

  /** Separates artifact URI from window-params suffix inside {@link #cacheMapKey}. */
  private static final char CACHE_KEY_SEP = '\u0000';

  private RadixArtifactReaderCache() {
  }

  static TempRadixArtifactReader getOrOpen(
      PartitionLookupDescriptor descriptor,
      StorageConfiguration<?> storageConf,
      RadixLookupWindowParams lookupWindowParams) {
    String artifactPath = descriptor.getArtifactPath();
    String mapKey = cacheMapKey(artifactPath, lookupWindowParams);

    ReaderHolder holder = CACHE.computeIfAbsent(mapKey, key -> {
      LOG.info(
          "RADIX reader cache create holder: partition={}, artifact={}, window={}",
          descriptor.getPartitionPath(),
          artifactPath,
          lookupWindowParams.cacheKeySuffix());
      return new ReaderHolder(descriptor, lookupWindowParams, mapKey);
    });

    return holder.getOrOpen(storageConf);
  }

  static TempRadixArtifactReader getOrOpen(
      PartitionLookupDescriptor descriptor,
      StorageConfiguration<?> storageConf,
      int lookupWindowKeys) {
    return getOrOpen(descriptor, storageConf, RadixLookupWindowParams.fixed(lookupWindowKeys));
  }

  static TempRadixArtifactReader getOrOpen(
      PartitionLookupDescriptor descriptor,
      StorageConfiguration<?> storageConf) {
    return getOrOpen(descriptor, storageConf, RadixLookupWindowParams.fixed(4096));
  }

  static void evict(String artifactPath) {
    Iterator<Map.Entry<String, ReaderHolder>> it = CACHE.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, ReaderHolder> entry = it.next();
      if (!artifactPathFromCacheMapKey(entry.getKey()).equals(artifactPath)) {
        continue;
      }
      ReaderHolder holder = entry.getValue();
      it.remove();
      holder.closeQuietly();
      LOG.info("RADIX reader cache evicted: artifact={}", artifactPath);
    }
  }

  /**
   * Closes cached readers whose artifact URI lies under {@literal .radix_index_tmp/instants/<instantTime>/}.
   * Invoked when rollback deletes that staging directory; a {@code latest} manifest may be absent.
   */
  static void evictArtifactsUnderRadixInstant(String instantTime) {
    if (instantTime == null || instantTime.isEmpty()) {
      return;
    }
    String needle = "/instants/" + instantTime + "/";
    Iterator<Map.Entry<String, ReaderHolder>> it = CACHE.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, ReaderHolder> entry = it.next();
      String artifactKey = artifactPathFromCacheMapKey(entry.getKey());
      if (!artifactKey.contains(needle)) {
        continue;
      }
      ReaderHolder holder = entry.getValue();
      it.remove();
      holder.closeQuietly();
      LOG.info(
          "RADIX reader cache evicted (rollback instant): instant={}, artifact={}",
          instantTime,
          artifactKey);
    }
  }

  /**
   * Closes and removes cached readers for radix artifacts under this table's staging directory
   * ({@literal <tableBase>/.hoodie/.radix_index_tmp/...}), matching production layout from
   * {@link StorageRadixArtifactPublisher}. Called from {@link HoodieRadixSplineIndex#close()} so that
   * closing the Spark write client (which invokes {@code HoodieIndex#close()}) releases seekable
   * streams; cache entries for other table bases in the same JVM are left intact.
   *
   * <p>Artifacts published outside that tree (e.g. some tests with ad-hoc file URIs) are not evicted
   * here — use {@link #clear()} in test teardown if needed.
   */
  static void evictForTable(String tableBasePath) {
    if (tableBasePath == null || tableBasePath.isEmpty()) {
      return;
    }
    StoragePath tableBase = new StoragePath(tableBasePath);
    StoragePath radixStaging = new StoragePath(tableBase, ".hoodie/.radix_index_tmp");
    String radixPathPrefix = normalizedUriPath(radixStaging);
    if (radixPathPrefix == null) {
      return;
    }

    Iterator<Map.Entry<String, ReaderHolder>> it = CACHE.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, ReaderHolder> entry = it.next();
      String artifactKey = artifactPathFromCacheMapKey(entry.getKey());
      if (artifactUnderRadixStaging(artifactKey, radixPathPrefix)) {
        ReaderHolder holder = entry.getValue();
        String partitionPath = holder.getPartitionPath();
        it.remove();
        holder.closeQuietly();
        LOG.info(
            "RADIX reader cache evicted (table scope): partition={}, artifact={}",
            partitionPath,
            artifactKey);
      }
    }
  }

  /**
   * Package-private for radix tests.
   */
  static int cacheSizeForTesting() {
    return CACHE.size();
  }

  private static String cacheMapKey(String artifactPath, RadixLookupWindowParams params) {
    return artifactPath + CACHE_KEY_SEP + params.cacheKeySuffix();
  }

  private static String artifactPathFromCacheMapKey(String mapKey) {
    int sep = mapKey.indexOf(CACHE_KEY_SEP);
    return sep < 0 ? mapKey : mapKey.substring(0, sep);
  }

  private static boolean artifactUnderRadixStaging(String artifactUri, String radixPathPrefix) {
    StoragePath artifactSp;
    try {
      artifactSp = new StoragePath(artifactUri);
    } catch (IllegalArgumentException e) {
      LOG.warn("RADIX reader cache: skip eviction for unparsable artifact URI={}", artifactUri, e);
      return false;
    }
    String artifactPath = normalizedUriPath(artifactSp);
    if (artifactPath == null) {
      return false;
    }
    return artifactPath.equals(radixPathPrefix)
        || artifactPath.startsWith(radixPathPrefix + StoragePath.SEPARATOR);
  }

  private static String normalizedUriPath(StoragePath path) {
    String p = path.toUri().getPath();
    if (p == null) {
      return null;
    }
    if (p.length() > 1 && p.endsWith(StoragePath.SEPARATOR)) {
      return p.substring(0, p.length() - 1);
    }
    return p;
  }

  static void evictIdleReaders() {
    long now = System.currentTimeMillis();

    Iterator<Map.Entry<String, ReaderHolder>> it = CACHE.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, ReaderHolder> entry = it.next();
      ReaderHolder holder = entry.getValue();

      if (holder.isIdleExpired(now, IDLE_TTL_MS)) {
        it.remove();
        holder.closeQuietly();
        LOG.info(
            "RADIX reader cache evicted idle reader: partition={}, artifact={}, idleMs={}",
            holder.getPartitionPath(),
            holder.getArtifactPath(),
            now - holder.getLastAccessTime());
      }
    }
  }

  static void clear() {
    Iterator<Map.Entry<String, ReaderHolder>> it = CACHE.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, ReaderHolder> entry = it.next();
      it.remove();
      entry.getValue().closeQuietly();
    }
    LOG.info("RADIX reader cache cleared");
  }

  private static final class ReaderHolder {
    private final PartitionLookupDescriptor descriptor;
    private final RadixLookupWindowParams lookupWindowParams;
    private final String mapKey;

    private volatile TempRadixArtifactReader reader;
    private volatile RuntimeException openFailure;
    private volatile long lastAccessTimeMs;

    private ReaderHolder(
        PartitionLookupDescriptor descriptor,
        RadixLookupWindowParams lookupWindowParams,
        String mapKey) {
      this.descriptor = descriptor;
      this.lookupWindowParams = lookupWindowParams;
      this.mapKey = mapKey;
      this.lastAccessTimeMs = System.currentTimeMillis();
    }

    TempRadixArtifactReader getOrOpen(StorageConfiguration<?> storageConf) {
      TempRadixArtifactReader cached = reader;
      if (cached != null) {
        lastAccessTimeMs = System.currentTimeMillis();
        return cached;
      }

      synchronized (this) {
        cached = reader;
        if (cached != null) {
          lastAccessTimeMs = System.currentTimeMillis();
          return cached;
        }

        if (openFailure != null) {
          throw openFailure;
        }

        long started = System.nanoTime();
        LOG.info(
            "RADIX opening reader: partition={}, artifact={}",
            descriptor.getPartitionPath(),
            descriptor.getArtifactPath());

        try {
          TempRadixArtifactReader opened =
              SimpleTempRadixArtifactReader.open(
                  descriptor.getArtifactPath(), storageConf, lookupWindowParams);

          reader = opened;
          lastAccessTimeMs = System.currentTimeMillis();

          LOG.info(
              "RADIX reader opened: partition={}, artifact={}, openMs={}",
              descriptor.getPartitionPath(),
              descriptor.getArtifactPath(),
              (System.nanoTime() - started) / 1_000_000L);

          return opened;
        } catch (IOException ioe) {
          RuntimeException failure = new RuntimeException(
              "Failed to open radix artifact reader for partition="
                  + descriptor.getPartitionPath()
                  + ", artifact=" + descriptor.getArtifactPath(),
              ioe);
          openFailure = failure;
          CACHE.remove(mapKey, this);
          throw failure;
        }
      }
    }

    boolean isIdleExpired(long nowMs, long ttlMs) {
      return reader != null && (nowMs - lastAccessTimeMs) >= ttlMs;
    }

    long getLastAccessTime() {
      return lastAccessTimeMs;
    }

    String getPartitionPath() {
      return descriptor.getPartitionPath();
    }

    String getArtifactPath() {
      return descriptor.getArtifactPath();
    }

    void closeQuietly() {
      synchronized (this) {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
            LOG.warn(
                "RADIX failed to close cached reader: partition={}, artifact={}",
                descriptor.getPartitionPath(),
                descriptor.getArtifactPath(),
                e);
          } finally {
            reader = null;
          }
        }
        openFailure = null;
      }
    }
  }
}