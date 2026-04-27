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
# Output artifacts are written to benchmark-sweep-results/<timestamp>/ by default:
#   - run.log     (full console log)
#   - summary.json (HUDI_BENCH_JSON_SUMMARY unless explicitly overridden)
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

RUN_ID="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${HUDI_BENCH_OUTPUT_DIR:-$ROOT/benchmark-sweep-results/$RUN_ID}"
mkdir -p "$OUT_DIR"

if [[ -z "${HUDI_BENCH_JSON_SUMMARY:-}" ]]; then
  if [[ "$OUT_DIR" == "$ROOT/"* ]]; then
    rel="${OUT_DIR#$ROOT/}"
    export HUDI_BENCH_JSON_SUMMARY="/workspace/hudi/$rel/summary.json"
  else
    export HUDI_BENCH_JSON_SUMMARY="/workspace/hudi/benchmark-sweep-results/$RUN_ID/summary.json"
  fi
fi

# Rebuild env list after defaults potentially set above.
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
  HUDI_BENCH_PARTITION_BUCKETS \
  HUDI_BUCKET_NUM_BUCKETS \
  HUDI_METADATA_INDEX_RADIX_SPLINE_ENABLE \
  HUDI_BENCH_JSON_SUMMARY \
  BENCH_SPARK_SHUFFLE_PARTITIONS \
  BENCH_SPARK_DEFAULT_PARALLELISM \
  BENCH_SPARK_DRIVER_MAX_RESULT_SIZE \
  BENCH_SPARK_EXECUTOR_MEMORY_OVERHEAD \
  BENCH_SPARK_EXECUTOR_MEMORY \
  BENCH_SPARK_DRIVER_MEMORY \
  SKIP_BUNDLE_BUILD \
  BENCH_SPARK_MASTER; do
  append_if_set "$name"
done

echo "Starting HDFS + Spark + hudi-dev..."
docker compose up -d

echo "Running benchmark inside hudi-dev..."
echo "Run log: $OUT_DIR/run.log"
echo "Summary JSON (container path): ${HUDI_BENCH_JSON_SUMMARY}"
set +e
docker compose exec -T "${DOCKER_ENV[@]}" hudi-dev \
  /workspace/hudi/scripts/run-baseline-benchmark-in-container.sh \
  "$@" 2>&1 | tee "$OUT_DIR/run.log"
rc=${PIPESTATUS[0]}
set -e
exit "$rc"
