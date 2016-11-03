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

source $dir/find-hive-dependency.sh

jobjar=`find ${KYLIN_HOME}/lib -name '*job*.jar'`
java -cp ${hive_dependency}:${jobjar}  org.apache.kylin.common.util.ClasspathScanner  HCatInputFormat.class  >/dev/null 2>&1
[[ $? == 0 ]]         || quit "ERROR: Class HCatInputFormat is not found on hive dependency. Please check bin/find-hive-dependency.sh is setup correctly."

# check snappy
need_snappy_lib=`sh $KYLIN_HOME/bin/get-properties.sh kap.storage.columnar.page.compression`
if [[ ${need_snappy_lib} == "SNAPPY" ]]
then
    snappy_lib_count=`echo ${hive_dependency} | grep -o "snappy-java" | wc -l`
    [[ ${snappy_lib_count} != 0 ]]  || quit "ERROR: Snappy lib is not found. Please add it to classpath"
fi
