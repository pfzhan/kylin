#!/bin/bash
# Kyligence Inc. License

dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

# set KYLIN_HOME with consideration for multiple instances that are on the same node
KYLIN_HOME=${KYLIN_HOME:-"${dir}/../"}
export KYLIN_HOME=`cd "$KYLIN_HOME"; pwd`
dir="$KYLIN_HOME/bin"

function quit {
	echo "$@"
	exit 1
}

jobjar=`find ${KYLIN_HOME}/lib -name '*job*.jar'`
export HBASE_CLASSPATH=${jobjar}
hbaseroot=$(hbase  org.apache.kylin.storage.hbase.util.PrintHBaseConfig  "hbase.rootdir"  2>/dev/null)

a=$(hdfs dfs -ls $hbaseroot  2>&1)        || echo "WARN: Cannot hdfs dfs -ls $hbaseroot. Some diagnosis feature will be disabled."
