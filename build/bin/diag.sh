#!/bin/bash
# Kyligence Inc. License

if [[ -z ${KYLIN_HOME} ]];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

version=`cat ${KYLIN_HOME}/VERSION | awk '{print $3}'`

function help {
    echo "usage: diag.sh [-help] | [{diag_options}]"
    echo "Example:"
    echo "1. extract 3 days(default value) diagnosis info to default export folder:"
    echo "> diag.sh"
    echo "2. generate full diagnosis packages"
    echo "> diag.sh -startTime 1567267200000 -endTime 1567353600000"
    echo "3. generate job diagnosis packages"
    echo "> diag.sh -job job_id"
    echo "4. skip extract metadata in diagnosis packages"
    echo "> diag.sh -includeMeta false (default true)"
    echo "5. show this usage:"
    echo "> diag.sh -help"
    return 1
}

function retrieveDependency() {
    # get kylin_hadoop_conf_dir
    if [[ -z ${kylin_hadoop_conf_dir} ]]; then
       source ${KYLIN_HOME}/sbin/prepare-hadoop-conf-dir.sh
    fi

    #retrive $KYLIN_EXTRA_START_OPTS
    source ${KYLIN_HOME}/sbin/setenv.sh

    DIAG_JAVA_OPTS="-Dkylin.home=${KYLIN_HOME}"
}

function prepareEnv {
    export KYLIN_CONFIG_FILE="${KYLIN_HOME}/conf/kylin.properties"
    export SPARK_HOME=${KYLIN_HOME}/spark

    echo "KYLIN_HOME is:${KYLIN_HOME}"
    echo "KYLIN_CONFIG_FILE is:${KYLIN_CONFIG_FILE}"
    echo "SPARK_HOME is:${SPARK_HOME}"

    retrieveDependency

    mkdir -p ${KYLIN_HOME}/logs
    source ${KYLIN_HOME}/sbin/do-check-and-prepare-spark.sh

    # init kerberos
    source ${KYLIN_HOME}/sbin/init-kerberos.sh
    initKerberosIfNeeded

    if [[ ! -f "/usr/bin/influxd" && ! -f "${KYLIN_HOME}/influxdb/usr/bin/influxd" ]];then
        INFLUXDB_HOME="${KYLIN_HOME}/influxdb"
        cd ${INFLUXDB_HOME}
        influx_files=(`ls influxdb-*.rpm`)
        if [[ ${#influx_files} -gt 0 ]]; then
            rpm2cpio ${influx_files[0]} | cpio -div
        fi
        cd -
    fi
}

function runTool() {
    prepareEnv

    if [[ -f ${KYLIN_HOME}/conf/kylin-tools-diag-log4j.properties ]]; then
        diag_log4j="file:${KYLIN_HOME}/conf/kylin-tools-diag-log4j.properties"
    else
        diag_log4j="file:${KYLIN_HOME}/tool/conf/kylin-tools-diag-log4j.properties"
    fi

    java -Xms${JAVA_VM_XMS} -Xmx${JAVA_VM_XMX} ${DIAG_JAVA_OPTS} -Dfile.encoding=UTF-8 -Dlog4j.configuration=${diag_log4j} -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} -Dhdp.version=current -cp "${kylin_hadoop_conf_dir}:${KYLIN_HOME}/lib/ext/*:${KYLIN_HOME}/tool/kap-tool-${version}.jar:${SPARK_HOME}/jars/*" $@
    exit $?
}

USER_OPTS="$@"
if [[ ${USER_OPTS} == *"-help"* ]]; then
    help
    exit 1
fi

DIAG_OPTS="${USER_OPTS}"
if [[ ${DIAG_OPTS} != *"-destDir"* ]]; then
    destDir="${KYLIN_HOME}/diag_dump/"
    mkdir -p ${destDir}

    DIAG_OPTS="${DIAG_OPTS} -destDir ${destDir}"
fi

if ([[ ${DIAG_OPTS} != *"-project"* ]] && [[ ${DIAG_OPTS} != *"-job"* ]]); then
    project="-all"
    DIAG_OPTS="${DIAG_OPTS} -project ${project}"
fi

if [[ "$1" == *"-job"* ]]; then
    runTool io.kyligence.kap.tool.JobDiagInfoCLI ${DIAG_OPTS}
else
    runTool io.kyligence.kap.tool.DiagClientCLI ${DIAG_OPTS}
fi
