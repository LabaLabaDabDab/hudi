#!/usr/bin/env bash
set -euo pipefail

/opt/scripts/configure-hadoop.sh
/opt/scripts/wait-for-port.sh "${HDFS_NAMENODE_HOST:-namenode}" "${HDFS_NAMENODE_RPC_PORT:-9000}" 180

export SPARK_MASTER_HOST="${SPARK_MASTER_HOST:-spark-master}"
export SPARK_MASTER_PORT="${SPARK_MASTER_PORT:-7077}"
export SPARK_MASTER_WEBUI_PORT="${SPARK_MASTER_WEBUI_PORT:-8080}"

exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
  --host "$SPARK_MASTER_HOST" \
  --port "$SPARK_MASTER_PORT" \
  --webui-port "$SPARK_MASTER_WEBUI_PORT"
