#!/usr/bin/env bash
set -euo pipefail

export HDFS_NAMENODE_HOST="${HDFS_NAMENODE_HOST:-namenode}"
export HDFS_NAMENODE_RPC_PORT="${HDFS_NAMENODE_RPC_PORT:-9000}"
export HDFS_NAMENODE_HTTP_PORT="${HDFS_NAMENODE_HTTP_PORT:-9870}"
export HDFS_REPLICATION="${HDFS_REPLICATION:-1}"

mkdir -p "$HADOOP_CONF_DIR"

envsubst < /opt/templates/core-site.xml.template > "$HADOOP_CONF_DIR/core-site.xml"
envsubst < /opt/templates/hdfs-site.xml.template > "$HADOOP_CONF_DIR/hdfs-site.xml"

cat > "$HADOOP_CONF_DIR/workers" <<WORKERS
spark-worker
WORKERS
