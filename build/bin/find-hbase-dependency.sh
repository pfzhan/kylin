#!/bin/bash
# Kyligence Inc. License

# source me

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo Retrieving hbase dependency...

hbase_classpath=`hbase classpath`    || quit "Command 'hbase classpath' does not work. Please check hbase is installed correctly."

# special handling for Amazon EMR, to prevent re-init of hbase-setenv
is_aws=`uname -r | grep amzn`
if [ -n "$is_aws" ] && [ -d "/usr/lib/oozie/lib" ]; then
    export HBASE_ENV_INIT="true"
fi

hbase_dependency=${hbase_classpath}
verbose "hbase_dependency is $hbase_dependency"
export hbase_dependency