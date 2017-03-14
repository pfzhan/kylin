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

if [ $# -eq 1 ] || [ $# -eq 2 ] || [ $# -eq 3 ]
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

    if [ ${#patient} -eq 36 ]; then
        source ${dir}/find-hive-dependency.sh

        #retrive $KYLIN_EXTRA_START_OPTS
        if [ -f "${dir}/setenv-tool.sh" ]
            then source ${dir}/setenv-tool.sh
        fi

        mkdir -p ${KYLIN_HOME}/ext
        export HBASE_CLASSPATH_PREFIX=${KYLIN_HOME}/conf:${KYLIN_HOME}/tool/*:${KYLIN_HOME}/ext/*:${HBASE_CLASSPATH_PREFIX}
        export HBASE_CLASSPATH=${HBASE_CLASSPATH}:${hive_dependency}

        hbase ${KYLIN_EXTRA_START_OPTS} \
        -Dlog4j.configuration=kylin-tool-log4j.properties \
        -Dcatalina.home=${tomcat_root} \
        org.apache.kylin.tool.JobDiagnosisInfoCLI \
        -jobId $patient \
        -destDir $destDir || exit 1
    else
        KYBOT_OPTS=""
        if [ $needUpload = "true" ]; then
            KYBOT_OPTS="-uploadToServer true"
        fi
        # will use kybot as system diagnosis
        sh ${KYLIN_HOME}/kybot/kybot.sh $KYBOT_OPTS -destDir $destDir || exit 1
    fi

    exit 0
else
    echo "usage: diag.sh Project|JobId [target_path]"
    exit 1
fi