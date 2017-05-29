#!/bin/bash
# Kyligence Inc. License
#title=Checking HBase Classpath

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking HBase classpath..."

metadataUrl=`${dir}/get-properties.sh kylin.metadata.url`
if [[ "${metadataUrl##*@}" != "hbase" ]]
then
    echo "Not HBase metadata ${metadataUrl}. Skip check."
    exit 0
fi


oCP=${HBASE_CLASSPATH}
oCPP=${HBASE_CLASSPATH_PREFIX}

export HBASE_CLASSPATH=MyClasspathForTest
export HBASE_CLASSPATH_PREFIX=MyClasspathPrefixForTest

a=`hbase classpath`                      || quit "ERROR: Command 'hbase classpath' does not work. Please check hbase is installed correctly."

[[ $a == MyClasspathPrefixForTest* ]]    || quit "ERROR: Command 'hbase' does not respect env var HBASE_CLASSPATH_PREFIX. Please check if HBASE_CLASSPATH_PREFIX is overwritten inside hbase shell."

[[ $a == *MyClasspathForTest* ]]         || quit "ERROR: Command 'hbase' does not respect env var HBASE_CLASSPATH. Please check if HBASE_CLASSPATH is overwritten inside hbase shell."

export HBASE_CLASSPATH=${oCP}
export HBASE_CLASSPATH_PREFIX=${oCPP}
