#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

if [ $# != 1 ]
then
    echo 'invalid input'
    exit -1
fi

if [[ $CI_MODE == 'true' ]]; then
    cd $dir
    tool_jar=$(ls ../../kylin/tool-assembly/target/kylin-tool-*-assembly.jar)
else
    tool_jar=$(ls $KYLIN_HOME/tool/kylin-tool-*.jar)
fi

kylin_conf_opts=
if [ ! -z "$KYLIN_CONF" ]; then
    kylin_conf_opts="-DKYLIN_CONF=$KYLIN_CONF"
fi
result=`$JAVA $kylin_conf_opts -cp $tool_jar -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties org.apache.kylin.tool.KylinConfigCLI $1 2>/dev/null`
echo "$result"