#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Host entrypoint: start docker compose stack and run baseline_benchmark.py
# inside hudi-dev (Spark 3.5.7 + Scala 2.12 + Java 17 — matches hudi-spark3.5-bundle_2.12).
#
# Usage (from repo root):
#   ./run-docker-baseline-benchmark.sh
#   export HUDI_INDEX_FILTER=RADIX_SPLINE N_INITIAL=200000
#   ./run-docker-baseline-benchmark.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker not found in PATH" >&2
  exit 2
fi

append_if_set() {
  local name="$1"
  if [[ -n "${!name+x}" ]]; then
    DOCKER_ENV+=(--env "$name=${!name}")
  fi
}

DOCKER_ENV=()
for name in \
  HUDI_BASE_ROOT \
  HUDI_SPARK_JARS \
  HUDI_INDEX_FILTER \
  HUDI_TABLE_NAME \
  N_INITIAL \
  N_UPDATES \
  N_INSERTS \
  ROUNDS \
  HUDI_CLEANUP_AFTER_PROFILE \
  HUDI_BENCH_LOG_LEVEL \
  HUDI_RADIX_PROFILE_TAG_LOCATION \
  HUDI_RADIX_MAX_ERROR \
  HUDI_RADIX_BITS \
  HUDI_RADIX_LOOKUP_WINDOW_KEYS \
  HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE \
  HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_MIN \
  HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_MAX \
  HUDI_RADIX_LOOKUP_WINDOW_ADAPTIVE_CALIBRATION_KEYS \
  HUDI_KEY_DISTRIBUTION \
  HUDI_BUCKET_NUM_BUCKETS \
  HUDI_METADATA_INDEX_RADIX_SPLINE_ENABLE \
  BENCH_SPARK_SHUFFLE_PARTITIONS \
  BENCH_SPARK_DEFAULT_PARALLELISM \
  BENCH_SPARK_DRIVER_MAX_RESULT_SIZE \
  BENCH_SPARK_EXECUTOR_MEMORY_OVERHEAD \
  BENCH_SPARK_EXECUTOR_MEMORY \
  BENCH_SPARK_DRIVER_MEMORY \
  SKIP_BUNDLE_BUILD; do
  append_if_set "$name"
done

echo "Starting HDFS + Spark + hudi-dev..."
docker compose up -d

echo "Running benchmark inside hudi-dev..."
exec docker compose exec -T "${DOCKER_ENV[@]}" hudi-dev \
  /workspace/hudi/scripts/run-baseline-benchmark-in-container.sh \
  "$@"
