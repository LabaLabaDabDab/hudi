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
# Run baseline_benchmark.py across hoodie.index.type values (Spark 3.5 in Docker).
# Default HUDI_INDEX_FILTER=ALL runs every COW-safe profile (see baseline_benchmark.py);
# BUCKET_CONSISTENT_HASHING is excluded unless you set an explicit filter that includes it.
#
# Override sizes from the host, e.g.:
#   N_INITIAL=500000 N_UPDATES=50000 ./run-docker-index-comparison.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

# ALL = every profile in baseline_benchmark.py that is safe for COPY_ON_WRITE (see COW_UNSAFE_PROFILE_IDS).
export HUDI_INDEX_FILTER="${HUDI_INDEX_FILTER:-ALL}"
export N_INITIAL="${N_INITIAL:-80000}"
export N_UPDATES="${N_UPDATES:-15000}"
export N_INSERTS="${N_INSERTS:-15000}"
export ROUNDS="${ROUNDS:-1}"
export HUDI_CLEANUP_AFTER_PROFILE="${HUDI_CLEANUP_AFTER_PROFILE:-true}"
export HUDI_BENCH_LOG_LEVEL="${HUDI_BENCH_LOG_LEVEL:-WARN}"
# Skip Maven if bundle already built on the host (mounted into the container).
export SKIP_BUNDLE_BUILD="${SKIP_BUNDLE_BUILD:-1}"

LOG="${ROOT}/target/index-comparison-$(date +%Y%m%d-%H%M%S).log"
mkdir -p "$(dirname "$LOG")"
echo "Logging to $LOG"
exec "$ROOT/run-docker-baseline-benchmark.sh" 2>&1 | tee "$LOG"
