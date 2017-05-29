#!/bin/bash
# Kyligence Inc. License
#title=Checking Permission of HBase's Root Dir

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking HBase read root dir..."

metadataUrl=`${dir}/get-properties.sh kylin.metadata.url`
if [[ "${metadataUrl##*@}" != "hbase" ]]
then
    echo "Not HBase metadata ${metadataUrl}. Skip check."
    exit 0
fi


jobjar=`find ${KYLIN_HOME}/lib -name '*job*.jar'`
export HBASE_CLASSPATH=${jobjar}
hbaseroot=$(hbase  org.apache.kylin.storage.hbase.util.PrintHBaseConfig  "hbase.rootdir")

hdfs dfs -ls $hbaseroot    || echo "${CHECKENV_REPORT_PFX}WARN: Cannot access $hbaseroot. Some diagnosis feature will be disabled."
