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
# Five baseline_benchmark runs: distinct HUDI_KEY_DISTRIBUTION values with
# N_INITIAL=5_000_000 (bulk keys). Tune N_UPDATES / N_INSERTS via env if needed.
#
# Usage (repo root, Docker stack up):
#   ./run-docker-benchmark-5datasets-5m.sh
#   SKIP_BUNDLE_BUILD=0 ./run-docker-benchmark-5datasets-5m.sh   # rebuild bundle in container first
#
# Requires enough Spark worker RAM (see docker-compose SPARK_WORKER_MEMORY); defaults
# below default to 2g executor + 2g driver — adjust BENCH_SPARK_* / compose worker memory if OOM.
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

export SKIP_BUNDLE_BUILD="${SKIP_BUNDLE_BUILD:-1}"
export HUDI_INDEX_FILTER="${HUDI_INDEX_FILTER:-RADIX_SPLINE}"
export N_INITIAL="${N_INITIAL:-5000000}"
export N_UPDATES="${N_UPDATES:-400000}"
export N_INSERTS="${N_INSERTS:-400000}"
export ROUNDS="${ROUNDS:-1}"
export HUDI_CLEANUP_AFTER_PROFILE="${HUDI_CLEANUP_AFTER_PROFILE:-true}"
export HUDI_BENCH_LOG_LEVEL="${HUDI_BENCH_LOG_LEVEL:-WARN}"

# Large-run Spark tuning (override from host if workers are smaller)
export BENCH_SPARK_SHUFFLE_PARTITIONS="${BENCH_SPARK_SHUFFLE_PARTITIONS:-256}"
export BENCH_SPARK_DEFAULT_PARALLELISM="${BENCH_SPARK_DEFAULT_PARALLELISM:-128}"
export BENCH_SPARK_DRIVER_MEMORY="${BENCH_SPARK_DRIVER_MEMORY:-2g}"
# Default 2g fits spark-worker SPARK_WORKER_MEMORY=4g in docker-compose; raise if you bump the worker.
export BENCH_SPARK_EXECUTOR_MEMORY="${BENCH_SPARK_EXECUTOR_MEMORY:-2g}"
export BENCH_SPARK_DRIVER_MAX_RESULT_SIZE="${BENCH_SPARK_DRIVER_MAX_RESULT_SIZE:-4g}"
export BENCH_SPARK_EXECUTOR_MEMORY_OVERHEAD="${BENCH_SPARK_EXECUTOR_MEMORY_OVERHEAD:-512m}"

LOGDIR="$ROOT/target/bench-5datasets-5m"
mkdir -p "$LOGDIR"
TS="$(date +%Y%m%d-%H%M%S)"
SUMMARY="$LOGDIR/summary-$TS.txt"

{
  echo "N_INITIAL=$N_INITIAL N_UPDATES=$N_UPDATES N_INSERTS=$N_INSERTS ROUNDS=$ROUNDS profile=$HUDI_INDEX_FILTER"
  echo "Spark: driver=$BENCH_SPARK_DRIVER_MEMORY executor=$BENCH_SPARK_EXECUTOR_MEMORY shuffle=$BENCH_SPARK_SHUFFLE_PARTITIONS"
  echo ""
} | tee "$SUMMARY"

for dist in linear quadratic affine7919 triangular poly_sum; do
  LOG="$LOGDIR/${dist}-${TS}.log"
  echo "======== dataset=$dist -> $LOG ========" | tee -a "$SUMMARY"
  set +e
  HUDI_KEY_DISTRIBUTION="$dist" "$ROOT/run-docker-baseline-benchmark.sh" 2>&1 | tee "$LOG"
  rc=${PIPESTATUS[0]}
  set -e
  echo "exit_rc=$rc" | tee -a "$SUMMARY"
  echo "" | tee -a "$SUMMARY"
done

echo "Done. Summary header: $SUMMARY"
