#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

if [ $# != 1 ]
then
    echo 'invalid input'
    exit -1
fi

tool_jar=$(ls $KYLIN_HOME/tool/kap-tool-*.jar)

kylin_conf_opts=
if [ ! -z "$KYLIN_CONF" ]; then
    kylin_conf_opts="-DKYLIN_CONF=$KYLIN_CONF"
fi
result=`$JAVA $kylin_conf_opts -cp $tool_jar -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties org.apache.kylin.tool.KylinConfigCLI $1 2>/dev/null`
echo "$result"