#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

jobjar=`find ${KYLIN_HOME}/lib -name '*job*.jar'`
export HBASE_CLASSPATH=${jobjar}
hbaseroot=$(hbase  org.apache.kylin.storage.hbase.util.PrintHBaseConfig  "hbase.rootdir"  2>/dev/null)

a=$(hdfs dfs -ls $hbaseroot  2>&1)        || echo "WARN: Cannot access $hbaseroot. Some diagnosis feature will be disabled."
