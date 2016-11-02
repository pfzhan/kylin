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

java -cp ${hive_dependency}:${KYLIN_HOME}/lib/*job*.jar  org.apache.kylin.common.util.ClasspathScanner  HCatInputFormat.class  >/dev/null 2>&1
[[ $? == 0 ]]         || quit "ERROR: Class HCatInputFormat is not found on hive dependency. Please check bin/find-hive-dependency.sh is setup correctly."

