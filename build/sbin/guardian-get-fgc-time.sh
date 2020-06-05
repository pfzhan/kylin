#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

PID_FILE=${KYLIN_HOME}/pid

if [[ -f ${PID_FILE} ]]; then
    PID=`cat ${PID_FILE}`
    GC_INFO=`jstat -gc ${PID}`
    array=(${GC_INFO})

    arr_len=${#array[*]}

    echo "${array[$arr_len - 2]}"
else
    echo "PID file is missing"
    exit 1
fi