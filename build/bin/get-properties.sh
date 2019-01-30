#!/bin/bash
# Kyligence Inc. License

if [ $# != 1 ]
then
    echo 'invalid input'
    exit -1
fi

if [ -z $KYLIN_HOME ];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

oldSkip=$SKIP_KERB
export SKIP_KERB=1
result=`${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.KylinConfigCLI $1 2>/dev/null`
export SKIP_KERB=$oldSkip

echo "$result"