#!/bin/bash
# Kyligence Inc. License

if [ $# != 1 ]
then
    if [[ $# < 2 || $2 != 'DEC' ]]
        then
            echo 'invalid input'
            exit 1
    fi
fi

if [ -z $KYLIN_HOME ];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

if [ -z ${kylin_hadoop_conf_dir} ]; then
    export kylin_hadoop_conf_dir=$KYLIN_HOME/hadoop_conf
fi

export KYLIN_KERBEROS_OPTS=""
if [ -f ${KYLIN_HOME}/conf/krb5.conf ];then
    KYLIN_KERBEROS_OPTS="-Djava.security.krb5.conf=${KYLIN_HOME}/conf/krb5.conf"
fi

export SPARK_HOME=$KYLIN_HOME/spark

tool_jar=$(ls $KYLIN_HOME/tool/kap-tool-*.jar)

result=`java ${KYLIN_KERBEROS_OPTS} -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} -Dhdp.version=current -cp "${kylin_hadoop_conf_dir}:${KYLIN_HOME}/lib/ext/*:$tool_jar:${SPARK_HOME}/jars/*" io.kyligence.kap.tool.KylinConfigCLI $@ 2>${KYLIN_HOME}/logs/shell.stderr`

echo "$result"