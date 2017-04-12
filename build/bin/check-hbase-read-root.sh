#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking HBase read root dir..."

jobjar=`find ${KYLIN_HOME}/lib -name '*job*.jar'`
export HBASE_CLASSPATH=${jobjar}
hbaseroot=$(hbase  org.apache.kylin.storage.hbase.util.PrintHBaseConfig  "hbase.rootdir")

hdfs dfs -ls $hbaseroot    || echo ">   : Cannot access $hbaseroot. Some diagnosis feature will be disabled."
