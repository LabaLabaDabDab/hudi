#!/usr/bin/env bash
set -euo pipefail

/opt/scripts/configure-hadoop.sh
/opt/scripts/wait-for-port.sh "${HDFS_NAMENODE_HOST:-namenode}" "${HDFS_NAMENODE_RPC_PORT:-9000}" 180
/opt/scripts/wait-for-port.sh "${SPARK_MASTER_HOST:-spark-master}" "${SPARK_MASTER_PORT:-7077}" 180

cat <<MSG

Hudi dev container is ready.

Useful commands:
  hdfs dfs -ls /
  /opt/scripts/init-hdfs.sh
  /opt/spark/bin/spark-shell --master spark://spark-master:7077 \
    --jars /workspace/hudi/packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.1.1.jar

MSG

exec bash
