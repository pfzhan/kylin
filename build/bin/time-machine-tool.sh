#!/bin/bash
# Kyligence Inc. License

set -x

if [ -z $KYLIN_HOME ];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi


function help(){
    echo "Usage: $0  [--time <travel_time(must required)>] [--project <project_name>] [--skip-check-data <skip_check_storage_data> ]"
    exit 1
}


mkdir -p ${KYLIN_HOME}/logs
ERR_LOG=${KYLIN_HOME}/logs/shell.stderr
OUT_LOG=${KYLIN_HOME}/logs/shell.stdout

PROJECT=
SKIP_CHECK_DATA_SECTION=
TIME_SECTION1=
TIME_SECTION2=
function main() {
    while [[ $# != 0 ]]; do
        if [[ $1 == "-t" || $1 == "--time" ]]; then
            TIME_SECTION1=$2
            TIME_SECTION2=$3
            TIME='$2 $3'
        elif [[ $1 == "-p" || $1 == "--project"  ]]; then
            PROJECT=$2
        elif [[ $1 == "--skip-check-data" ]]; then
            SKIP_CHECK_DATA_SECTION='-skipCheckData true'
        fi
        shift
    done
    if [[ -z $TIME_SECTION1 || -z TIME_SECTION2 ]]; then
      echo "Specify the travel time(must required)"
        help
    fi
    echo "io.kyligence.kap.tool.TimeMachineTool -time '$TIME_SECTION1 $TIME_SECTION2' $PROJECT_SECTION $SKIP_CHECK_DATA_SECTION"

    source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh $@
    version=`cat ${KYLIN_HOME}/VERSION | awk '{print $3}'`
    ${KYLIN_HOME}/sbin/rotate-logs.sh $@

    if [ "$1" == "-v" ]; then
        shift
    fi

    source ${KYLIN_HOME}/sbin/setenv.sh
    source ${KYLIN_HOME}/sbin/prepare-hadoop-conf-dir.sh
    export SPARK_HOME=${KYLIN_HOME}/spark

    java -Xms${JAVA_VM_XMS} -Xmx${JAVA_VM_XMX} -cp "${kylin_hadoop_conf_dir}:${KYLIN_HOME}/lib/ext/*:${KYLIN_HOME}/tool/kap-tool-$version.jar:${SPARK_HOME}/jars/*" io.kyligence.kap.tool.TimeMachineTool -time "$TIME_SECTION1 $TIME_SECTION2" 2>>${ERR_LOG}  | tee -a ${OUT_LOG}
}

main $@