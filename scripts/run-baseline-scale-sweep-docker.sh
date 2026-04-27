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
# Scale sweep for baseline_benchmark.py (Docker): fixed partition fan-out,
# several N_INITIAL values, all COW-safe index profiles (same as empty HUDI_INDEX_FILTER).
#
# Default sizes (keys ~ rows from bulk load): 100k, 500k, 1M, 5M.
# Override list (space-separated integers): SWEEP_SIZES="100000 250000 ..."
#
# Each run sets N_UPDATES ≈ 25% N_INITIAL, N_INSERTS ≈ 10% N_INITIAL.
#
# Usage (repo root):
#   chmod +x scripts/run-baseline-scale-sweep-docker.sh
#   ./scripts/run-baseline-scale-sweep-docker.sh
#
# Optional:
#   SWEEP_SIZES="100000 500000 1000000 2000000 5000000" SWEEP_PARTITION_BUCKETS=100 ./scripts/run-baseline-scale-sweep-docker.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SWEEP_SIZES_DEFAULT="100000 500000 1000000 5000000"
SIZES_STR="${SWEEP_SIZES:-$SWEEP_SIZES_DEFAULT}"

RUN_ID="${SWEEP_RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
OUT_HOST="${BASE_SWEEP_OUT_DIR:-$ROOT/benchmark-sweep-results}/$RUN_ID"
mkdir -p "$OUT_HOST"

echo "Scale sweep: results under $OUT_HOST"
echo "Partition buckets (fixed): ${SWEEP_PARTITION_BUCKETS:-100}"
echo "N_INITIAL sequence: $SIZES_STR"
echo "---"

shuffle_for_n() {
  local n="$1"
  if (( n <= 100000 )); then echo 200
  elif (( n <= 500000 )); then echo 400
  elif (( n <= 1000000 )); then echo 600
  elif (( n <= 2000000 )); then echo 800
  else echo 1000
  fi
}

parallelism_for_n() {
  local n="$1"
  local sp
  sp="$(shuffle_for_n "$n")"
  echo $(( sp / 2 ))
}

for n in $SIZES_STR; do
  if ! [[ "$n" =~ ^[0-9]+$ ]] || (( n < 1 )); then
    echo "SKIP invalid size token: $n" >&2
    continue
  fi
  NU=$(( n * 25 / 100 ))
  NI=$(( n * 10 / 100 ))
  SP="$(shuffle_for_n "$n")"
  PAR="$(parallelism_for_n "$n")"
  REL_JSON="benchmark-sweep-results/$RUN_ID/n_${n}.json"
  JSON_CONTAINER="/workspace/hudi/$REL_JSON"

  echo ""
  echo "========== N_INITIAL=$n  N_UPDATES=$NU  N_INSERTS=$NI  shuffle=$SP =========="

  export N_INITIAL="$n"
  export N_UPDATES="$NU"
  export N_INSERTS="$NI"
  export HUDI_BENCH_PARTITION_BUCKETS="${SWEEP_PARTITION_BUCKETS:-100}"
  export ROUNDS="${SWEEP_ROUNDS:-1}"
  unset HUDI_INDEX_FILTER
  export HUDI_BENCH_JSON_SUMMARY="$JSON_CONTAINER"
  export BENCH_SPARK_SHUFFLE_PARTITIONS="$SP"
  export BENCH_SPARK_DEFAULT_PARALLELISM="$PAR"
  # Prefer local[k] so benchmarks finish even when Spark standalone workers reject executors (memory/core mismatch).
  export BENCH_SPARK_MASTER="${SWEEP_SPARK_MASTER:-local[2]}"
  export BENCH_SPARK_EXECUTOR_MEMORY="${SWEEP_EXECUTOR_MEMORY:-3g}"
  export BENCH_SPARK_DRIVER_MEMORY="${SWEEP_DRIVER_MEMORY:-3g}"
  export BENCH_SPARK_DRIVER_MAX_RESULT_SIZE="${SWEEP_DRIVER_MAX_RESULT_SIZE:-4g}"
  export SKIP_BUNDLE_BUILD="${SKIP_BUNDLE_BUILD:-1}"

  mkdir -p "$(dirname "$OUT_HOST/n_${n}.json")"

  LOG_FILE="$OUT_HOST/run_n${n}.log"
  set +e
  ./run-docker-baseline-benchmark.sh 2>&1 | tee "$LOG_FILE"
  RC=${PIPESTATUS[0]}
  set -e

  HOST_JSON="$OUT_HOST/n_${n}.json"
  if [[ -f "$HOST_JSON" ]]; then
    echo "JSON: $HOST_JSON"
  else
    echo "WARN: expected JSON not found at $HOST_JSON (benchmark may have failed early)" >&2
  fi

  if [[ "$RC" != 0 ]]; then
    echo "WARN: run for N_INITIAL=$n exited with $RC" >&2
  fi
done

echo ""
echo "Done. Aggregate with:"
echo "  python3 $ROOT/scripts/aggregate-baseline-sweep-json.py $OUT_HOST/*.json > $OUT_HOST/summary.md"
