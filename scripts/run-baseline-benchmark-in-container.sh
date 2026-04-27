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
# Run inside the hudi-dev Docker container (Spark 3.5.7 + Scala 2.12 + Java 17):
#   docker compose exec hudi-dev /workspace/hudi/scripts/run-baseline-benchmark-in-container.sh
#
# Optional env (same as baseline_benchmark.py), e.g.:
#   HUDI_INDEX_FILTER=RADIX_SPLINE N_INITIAL=100000 docker compose exec -e HUDI_INDEX_FILTER -e N_INITIAL ...
#
set -euo pipefail

HUDI_REPO="${HUDI_REPO:-/workspace/hudi}"
cd "$HUDI_REPO"

if [[ -z "${JAVA_HOME:-}" && -d /opt/java/openjdk ]]; then
  export JAVA_HOME=/opt/java/openjdk
fi
export PATH="${JAVA_HOME}/bin:${PATH}"

/opt/scripts/configure-hadoop.sh
/opt/scripts/wait-for-port.sh "${HDFS_NAMENODE_HOST:-namenode}" "${HDFS_NAMENODE_RPC_PORT:-9000}" 180
/opt/scripts/wait-for-port.sh "${SPARK_MASTER_HOST:-spark-master}" "${SPARK_MASTER_PORT:-7077}" 180

# init-hdfs uses chmod on HDFS paths; that fails while the namenode is in safe mode.
echo "Waiting for HDFS safe mode to clear (up to 120s)..."
safe_off=0
for _ in $(seq 1 120); do
  if hdfs dfsadmin -safemode get 2>&1 | grep -q 'Safe mode is OFF'; then
    safe_off=1
    break
  fi
  sleep 1
done
if [[ "$safe_off" != "1" ]]; then
  echo "WARN: safe mode still ON after 120s — leaving safe mode (single-node dev only)."
  hdfs dfsadmin -safemode leave || true
fi

/opt/scripts/init-hdfs.sh

if [[ "${SKIP_BUNDLE_BUILD:-}" != "1" ]]; then
  echo "Building hudi-spark3.5-bundle (Scala 2.12) — first run may take several minutes..."
  # Hudi Spark 3.5 / Scala 2.12 bundle: build with JDK 17 (image hudi-dev uses Java 17).
  mvn -Dspark3.5 -Dscala-2.12 -pl packaging/hudi-spark-bundle -am -DskipTests package -Dmaven.javadoc.skip=true
fi

shopt -s nullglob
candidates=(packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-*.jar)
bundle=""
for j in "${candidates[@]}"; do
  [[ "$j" == *-sources.jar ]] && continue
  [[ "$j" == *-javadoc.jar ]] && continue
  [[ "$j" == *original-* ]] && continue
  bundle="$j"
  break
done
shopt -u nullglob

if [[ -z "$bundle" || ! -f "$bundle" ]]; then
  echo "ERROR: no hudi-spark3.5-bundle_2.12-*.jar under packaging/hudi-spark-bundle/target" >&2
  exit 2
fi

export HUDI_SPARK_JARS="$bundle"
export HUDI_BASE_ROOT="${HUDI_BASE_ROOT:-hdfs://namenode:9000/user/hudi/trips_cow}"

echo "Using bundle: $bundle"
echo "HUDI_BASE_ROOT=$HUDI_BASE_ROOT"

SPARK_BENCH_CONF_ARGS=()
if [[ -n "${BENCH_SPARK_EXECUTOR_MEMORY:-}" ]]; then
  SPARK_BENCH_CONF_ARGS+=(--conf "spark.executor.memory=${BENCH_SPARK_EXECUTOR_MEMORY}")
fi
if [[ -n "${BENCH_SPARK_DRIVER_MEMORY:-}" ]]; then
  SPARK_BENCH_CONF_ARGS+=(--conf "spark.driver.memory=${BENCH_SPARK_DRIVER_MEMORY}")
fi

# BENCH_SPARK_MASTER — optional override e.g. local[2] when standalone workers do not offer resources.
SPARK_MASTER_URL="${BENCH_SPARK_MASTER:-spark://${SPARK_MASTER_HOST:-spark-master}:${SPARK_MASTER_PORT:-7077}}"

exec /opt/spark/bin/spark-submit \
  --master "$SPARK_MASTER_URL" \
  --conf "spark.hadoop.fs.defaultFS=hdfs://${HDFS_NAMENODE_HOST:-namenode}:${HDFS_NAMENODE_RPC_PORT:-9000}" \
  "${SPARK_BENCH_CONF_ARGS[@]}" \
  --jars "$bundle" \
  "$HUDI_REPO/baseline_benchmark.py" \
  "$@"
