#!/usr/bin/env bash
set -euo pipefail

/opt/scripts/configure-hadoop.sh
/opt/scripts/wait-for-port.sh "${HDFS_NAMENODE_HOST:-namenode}" "${HDFS_NAMENODE_RPC_PORT:-9000}" 180

hdfs dfs -mkdir -p /spark-events /tmp /user/root /user/hudi /warehouse || true
hdfs dfs -chmod -R 1777 /tmp || true
hdfs dfs -chmod -R 777 /spark-events /warehouse || true

echo "HDFS initialized"
