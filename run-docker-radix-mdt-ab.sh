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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# A/B baseline_benchmark.py: RADIX_SPLINE with hoodie.metadata.index.radix_spline.enable
# false vs true (manifest partition in MDT). Rebuild the Spark bundle after changing Java defaults.
#
# Usage (repo root):
#   ./run-docker-radix-mdt-ab.sh
#   N_INITIAL=200000 N_UPDATES=40000 N_INSERTS=40000 ROUNDS=2 ./run-docker-radix-mdt-ab.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

export SKIP_BUNDLE_BUILD="${SKIP_BUNDLE_BUILD:-1}"
export HUDI_INDEX_FILTER="${HUDI_INDEX_FILTER:-RADIX_SPLINE}"
export N_INITIAL="${N_INITIAL:-80000}"
export N_UPDATES="${N_UPDATES:-15000}"
export N_INSERTS="${N_INSERTS:-15000}"
export ROUNDS="${ROUNDS:-1}"
export HUDI_CLEANUP_AFTER_PROFILE="${HUDI_CLEANUP_AFTER_PROFILE:-true}"
export HUDI_BENCH_LOG_LEVEL="${HUDI_BENCH_LOG_LEVEL:-WARN}"

LOGDIR="$ROOT/target"
mkdir -p "$LOGDIR"
LOG="$LOGDIR/radix-mdt-ab-$(date +%Y%m%d-%H%M%S).log"
echo "Logging to $LOG"

{
  echo "======== Run A: hoodie.metadata.index.radix_spline.enable=false ========"
  HUDI_METADATA_INDEX_RADIX_SPLINE_ENABLE=false "$ROOT/run-docker-baseline-benchmark.sh"
  echo ""
  echo "======== Run B: hoodie.metadata.index.radix_spline.enable=true ========"
  HUDI_METADATA_INDEX_RADIX_SPLINE_ENABLE=true "$ROOT/run-docker-baseline-benchmark.sh"
} 2>&1 | tee "$LOG"

echo "Wrote $LOG"
