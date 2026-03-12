#!/usr/bin/env bash
set -euo pipefail

/opt/scripts/configure-hadoop.sh

if [ ! -f /data/dfs/name/current/VERSION ]; then
  echo "Formatting NameNode metadata..."
  hdfs namenode -format -nonInteractive -force
fi

exec hdfs namenode
