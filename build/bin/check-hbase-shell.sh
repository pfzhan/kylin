#!/bin/bash
# Kyligence Inc. License

dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

function quit {
	echo "$@" Please check your hbase shell.
	exit 1
}

oCP=${HBASE_CLASSPATH}
oCPP=${HBASE_CLASSPATH_PREFIX}

export HBASE_CLASSPATH=MyClasspathForTest
export HBASE_CLASSPATH_PREFIX=MyClasspathPrefixForTest

a=`hbase classpath`                      || quit "ERROR: Command 'hbase classpath' does not work."

[[ $a == MyClasspathPrefixForTest* ]]    || quit "ERROR: Command 'hbase' does not respect env var HBASE_CLASSPATH_PREFIX."

[[ $a == *MyClasspathForTest* ]]         || quit "ERROR: Command 'hbase' does not respect env var HBASE_CLASSPATH."

export HBASE_CLASSPATH=${oCP}
export HBASE_CLASSPATH_PREFIX=${oCPP}
