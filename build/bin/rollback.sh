#!/bin/bash

##
## Copyright (C) 2020 Kyligence Inc. All rights reserved.
##
## http://kyligence.io
##
## This software is the confidential and proprietary information of
## Kyligence Inc. ("Confidential Information"). You shall not disclose
## such Confidential Information and shall use it only in accordance
## with the terms of the license agreement you entered into with
## Kyligence Inc.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
## "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
## LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
## A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
## OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
## SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
## LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
## DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
## THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
## (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
## OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
##

if [ -z $KYLIN_HOME ];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi


function help(){
    echo "Usage: $0  [--time <travel_time(must required)>] [--project <project_name>] [--skip-check-data <skip_check_storage_data> ]"
    exit 1
}

function printBackupResult() {
    error=$1
    if [[ $error == 0 ]]; then
        echo "rollback success"
    else
        echo -e "rollback failed please check logs/shell.stderr and  logs/shell.stdout for reason"
    fi
}


mkdir -p ${KYLIN_HOME}/logs
ERR_LOG=${KYLIN_HOME}/logs/shell.stderr
OUT_LOG=${KYLIN_HOME}/logs/shell.stdout

PROJECT_SECTION=
SKIP_CHECK_DATA_SECTION=
TIME=
function main() {
    while [[ $# != 0 ]]; do
        if [[ $1 == "-t" || $1 == "--time" ]]; then
            TIME="$2"
        elif [[ $1 == "-p" || $1 == "--project"  ]]; then
            PROJECT_SECTION="-project $2"
        elif [[ $1 == "--skip-check-data" ]]; then
            SKIP_CHECK_DATA_SECTION="-skipCheckData true"
        fi
        shift
    done
    if [[ -z $TIME ]]; then
      echo "Specify the travel time(must required)"
        help
    fi
    echo $PROJECT_SECTION
    echo $SKIP_CHECK_DATA_SECTION
    echo "io.kyligence.kap.tool.RollbackTool -time '$TIME' $PROJECT_SECTION $SKIP_CHECK_DATA_SECTION"

    source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh "$@"
    version=`cat ${KYLIN_HOME}/VERSION | awk '{print $3}'`
    ${KYLIN_HOME}/sbin/rotate-logs.sh "$@"

    if [ "$1" == "-v" ]; then
        shift
    fi

    source ${KYLIN_HOME}/sbin/setenv.sh
    source ${KYLIN_HOME}/sbin/prepare-hadoop-conf-dir.sh
    export SPARK_HOME=${KYLIN_HOME}/spark

    java -Xms${JAVA_VM_XMS} -Xmx${JAVA_VM_XMX} -cp "${kylin_hadoop_conf_dir}:${KYLIN_HOME}/lib/ext/*:${KYLIN_HOME}/tool/kap-tool-$version.jar:${SPARK_HOME}/jars/*" io.kyligence.kap.tool.RollbackTool -time "$TIME"  $PROJECT_SECTION $SKIP_CHECK_DATA_SECTION 2>>${ERR_LOG}  | tee -a ${OUT_LOG}

    printBackupResult ${PIPESTATUS[0]}
}

main "$@"