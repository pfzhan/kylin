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

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh $@
guardian_enable=`${KYLIN_HOME}/bin/get-properties.sh kylin.guardian.enabled`

time_format='+%Y-%m-%d %H:%M:%S %Z'

function startKG() {
    if [[ ${guardian_enable} != "true" ]]; then
        return 0
    fi

    mkdir -p ${KYLIN_HOME}/logs

    echo `date "${time_format} "`"Starting KE guardian process..."

    ### lock the start process
    LOCK_NAME="$KYLIN_HOME/bin/kg-start.lock"
    if ( set -o noclobber; echo "$$" > "$LOCK_NAME") 2> /dev/null
    then
    trap 'rm -f "$LOCK_NAME"; exit $?' INT TERM EXIT

    KGID_FILE=${KYLIN_HOME}/kgid
    if [[ -f ${KGID_FILE} ]]; then
        PID=`cat ${KGID_FILE}`
        if ps -p ${PID} > /dev/null; then
            quit "KE guardian process is running, stop it first"
        fi
    fi

    KE_PID_FILE=${KYLIN_HOME}/pid
    if [[ ! -f ${KE_PID_FILE} ]]; then
        quit "Kyligence Enterprise is not running, will not start guardian process"
    fi

    if [[ -f ${KYLIN_HOME}/conf/kylin-guardian-log4j.xml ]]; then
        guardian_log4j="file:${KYLIN_HOME}/conf/kylin-guardian-log4j.xml"
    else
        guardian_log4j="file:${KYLIN_HOME}/server/conf/kylin-guardian-log4j.xml"
    fi

    TIME_ZONE=`${KYLIN_HOME}/bin/get-properties.sh kylin.web.timezone`
    if [[ -n ${TIME_ZONE} ]]; then
        TIME_ZONE="-Duser.timezone=${TIME_ZONE}"
    fi

    TOOL_OPTS="-Dfile.encoding=UTF-8 -Dkylin.home=${KYLIN_HOME} -Dlog4j.configurationFile=${guardian_log4j} ${TIME_ZONE}"
    TOOL_CLASSPATH=${KYLIN_HOME}/conf:${KYLIN_HOME}/lib/ext/*:${KYLIN_HOME}/server/jars/*:${SPARK_HOME}/jars/*

    nohup java -Xms128m -Xmx1g ${TOOL_OPTS} -classpath ${TOOL_CLASSPATH} io.kyligence.kap.tool.daemon.KapGuardian > /dev/null 2>&1 & echo $! > ${KYLIN_HOME}/kgid &

    PID=`cat ${KYLIN_HOME}/kgid`
    echo `date "${time_format} "`" new guardian process pid is "${PID} >> ${KYLIN_HOME}/logs/guardian.log

    echo `date "${time_format} "`"KE guardian process is started"
    echo `date "${time_format} "`"Check log in ${KYLIN_HOME}/logs/guardian.log"

    ### Removing lock
    rm -f ${LOCK_NAME}
    trap - INT TERM EXIT
    else
        quit "Failed to acquire lockfile: ${LOCK_NAME}. It might have been starting in another shell."
    fi

    return 0
}

function stopKG() {
    if [[ ${guardian_enable} != "true" ]]; then
        return 0
    fi

    echo `date "${time_format} "`"Stopping KE guardian process..."

    PID_FILE=${KYLIN_HOME}/kgid

    if [[ -f ${PID_FILE} ]]; then
        PID=`cat ${PID_FILE}`
        if ps -p ${PID} > /dev/null; then
            echo `date "${time_format} "`"Stopping KG process: ${PID}"
            kill ${PID}

            for i in {1..10}; do
              sleep 3
              if ps -p ${PID} > /dev/null; then
                 if [[ "$i" == "10" ]]; then
                    echo `date "${time_format} "`"Killing KG process: ${PID}"
                    kill -9 ${PID}
                 fi
                 continue
              fi
              break
           done

           rm ${PID_FILE}
           return 0
        fi
    fi

    echo `date "${time_format} "`"KE guardian process is not running"
    return 1
}

if [[ $1 == "start" ]]; then
    startKG
elif [[ $1 == "stop" ]]; then
    stopKG
elif [[ $1 == "kill" ]]; then
    echo `date "${time_format} "`"Killing Kyligence Enterprise, caused by OOM!"

    # stop KE
    PID_FILE=${KYLIN_HOME}/pid
    if [[ -f ${PID_FILE} ]]; then
        PID=`cat ${PID_FILE}`
        if ps -p ${PID} > /dev/null; then
           echo `date "${time_format} "`"Stopping Kylin: $PID"
           kill ${PID}
           for i in {1..10}; do
              sleep 3
              if ps -p ${PID} -f | grep kylin > /dev/null; then
                 if [[ "$i" == "10" ]]
                 then
                    echo `date "${time_format} "`"Killing Kylin: $PID"
                    kill -9 ${PID}
                 fi
                 continue
              fi
              break
           done
           exit 0
        fi
    fi
    quit `date "${time_format} "`"Kyligence Enterprise is not running"
else
    quit "Usage: 'guardian.sh start' or 'guardian.sh stop' or 'guardian.sh kill'"
fi