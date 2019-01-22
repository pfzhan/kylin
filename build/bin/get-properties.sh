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

tool_jar=$(ls $KYLIN_HOME/tool/kap-tool-*.jar)

result=`java -cp $tool_jar -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties io.kyligence.kap.tool.KylinConfigCLI $1 2>/dev/null`

echo "$result"