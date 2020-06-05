#!/bin/bash
# Kyligence Inc. License

## DEFAULT: get ke process status
## return code
## 0 process is running
## 1 process is stopped
## -1 process is crashed

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

PID_FILE=$1

if [[ -z ${PID_FILE} ]]; then
    PID_FILE=${KYLIN_HOME}/pid
fi

if [[ -f ${PID_FILE} ]]; then
    PID=`cat ${PID_FILE}`
    PROCESS_COUNT=`ps -p ${PID} | grep "${PID}" | wc -l`

    if [[ ${PROCESS_COUNT} -lt 1 ]]; then
        echo "-1"
    else
        echo "0"
    fi
else
    echo "1"
fi