#!/bin/bash
# Kyligence Inc. License

# We should set KYLIN_HOME here for multiple tomcat instances that are on the same node.
# In addition, we should set a KYLIN_HOME for the global use as normal.
KYLIN_HOME=`dirname $0`/..
export KYLIN_HOME=`cd "$KYLIN_HOME"; pwd`
dir="$KYLIN_HOME/bin"

mkdir -p ${KYLIN_HOME}/logs

tomcat_root=${dir}/../tomcat
export tomcat_root

if [ $# -gt 0 ] && [ $# -lt 13 ]
then
    patient="$1"
    if [ -z "$patient" ]
    then
        echo "You need to specify a Project or Job Id for diagnosis."
        exit 1
    fi
    destDir="$2"
    if [ -z "$destDir" ]
    then
        destDir="$KYLIN_HOME/diagnosis_dump/"
        mkdir -p $destDir
    fi
    needUpload="$3"
    startTime="$4"
    endTime="$5"
    currTime="$6"
    base="$7"
    storage="$8"
    query="$9"
    meta="${10}"
    job="${11}"
    others="${12}"

    KYBOT_OPTS=""
    if [ "$needUpload" == "true" ]; then
        KYBOT_OPTS="${KYBOT_OPTS} -uploadToServer true"
    fi

    if [ "$startTime" != "-1" ]
    then
        KYBOT_OPTS="${KYBOT_OPTS} -startTime $startTime"
    fi

    if [ "$endTime" != "-1" ]
    then
        KYBOT_OPTS="${KYBOT_OPTS} -endTime $endTime"
    fi

    if [ "$currTime" != "-1" ]
    then
        KYBOT_OPTS="${KYBOT_OPTS} -currentTime $currTime"
    fi

    if [ "$base" == "true" ]; then
        KYBOT_OPTS="${KYBOT_OPTS} -base true"
    fi

    if [ "$storage" == "true" ]; then
        KYBOT_OPTS="${KYBOT_OPTS} -storage true"
    fi

    if [ "$query" == "true" ]; then
        KYBOT_OPTS="${KYBOT_OPTS} -query true"
    fi

    if [ "$meta" == "true" ]; then
        KYBOT_OPTS="${KYBOT_OPTS} -meta true"
    fi

    if [ "$job" == "true" ]; then
        KYBOT_OPTS="${KYBOT_OPTS} -job true"
    fi

    if [ "$others" == "true" ]; then
        KYBOT_OPTS="${KYBOT_OPTS} -others true"
    fi

    if [ ${#patient} -eq 36 ]; then
        KYBOT_OPTS="${KYBOT_OPTS} -jobId ${patient}"
    fi

    # will use kybot as system diagnosis
    sh ${KYLIN_HOME}/kybot/kybot.sh $KYBOT_OPTS -destDir $destDir || exit 1

    exit 0
else
    echo "usage: diag.sh Project|JobId [target_path]"
    exit 1
fi