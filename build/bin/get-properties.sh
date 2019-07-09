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

if [ -z ${kylin_hadoop_conf_dir} ]; then
    export kylin_hadoop_conf_dir=$KYLIN_HOME/hadoop_conf
fi

if [ -z $SPARK_HOME ];then
    export SPARK_HOME=$KYLIN_HOME/spark
fi

tool_jar=$(ls $KYLIN_HOME/tool/kap-tool-*.jar)

result=`java -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} -Dhdp.version=current -cp "${kylin_hadoop_conf_dir}:${KYLIN_HOME}/lib/ext/*:$tool_jar:${SPARK_HOME}/jars/*" io.kyligence.kap.tool.KylinConfigCLI $1 2>/dev/null`

echo "$result"