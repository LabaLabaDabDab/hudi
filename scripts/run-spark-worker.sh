#!/usr/bin/env bash
set -euo pipefail

/opt/scripts/configure-hadoop.sh
/opt/scripts/wait-for-port.sh "${HDFS_NAMENODE_HOST:-namenode}" "${HDFS_NAMENODE_RPC_PORT:-9000}" 180
/opt/scripts/wait-for-port.sh "${SPARK_MASTER_HOST:-spark-master}" "${SPARK_MASTER_PORT:-7077}" 180

export SPARK_WORKER_WEBUI_PORT="${SPARK_WORKER_WEBUI_PORT:-8081}"
export SPARK_WORKER_CORES="${SPARK_WORKER_CORES:-2}"
export SPARK_WORKER_MEMORY="${SPARK_WORKER_MEMORY:-4g}"

exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --webui-port "$SPARK_WORKER_WEBUI_PORT" \
  --cores "$SPARK_WORKER_CORES" \
  --memory "$SPARK_WORKER_MEMORY" \
  spark://${SPARK_MASTER_HOST:-spark-master}:${SPARK_MASTER_PORT:-7077}
