#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh $@
version=`cat ${KYLIN_HOME}/VERSION | awk '{print $2}'`
${KYLIN_HOME}/bin/rotate-logs.sh $@

if [ "$1" == "-v" ]; then
    shift
fi

function prepareEnv {
    export KYLIN_CONFIG_FILE="${KYLIN_HOME}/conf/kylin.properties"
    export SPARK_HOME=${KYLIN_HOME}/spark

    verbose "KYLIN_HOME is:${KYLIN_HOME}"
    verbose "KYLIN_CONFIG_FILE is:${KYLIN_CONFIG_FILE}"
    verbose "SPARK_HOME is:${SPARK_HOME}"

    retrieveDependency

    mkdir -p ${KYLIN_HOME}/logs
    source ${KYLIN_HOME}/bin/replace-jars-under-spark.sh

    # init kerberos
    if [ "$SKIP_KERB" != "1" ]; then
        source ${KYLIN_HOME}/bin/init-kerberos.sh
        initKerberosIfNeeded
    fi
}

function retrieveDependency() {
    # get kylin_hadoop_conf_dir
    if [[ -z ${kylin_hadoop_conf_dir} ]]; then
       source ${dir}/prepare-hadoop-conf-dir.sh
    fi

    #retrive $KYLIN_EXTRA_START_OPTS
    if [ -f "${KYLIN_HOME}/conf/setenv.sh" ]; then
        source ${KYLIN_HOME}/conf/setenv.sh
        export KYLIN_EXTRA_START_OPTS=`echo ${KYLIN_EXTRA_START_OPTS}|sed  "s/-XX:+PrintFlagsFinal//g"`
    fi
}

function checkRestPort() {
    port=`$KYLIN_HOME/bin/get-properties.sh server.port`
    used=`netstat -tpln | grep "\<$port\>" | awk '{print $7}' | sed "s/\// /g"`
    if [ ! -z "$used" ]; then
        echo "<$used> already listen on $port"
        exit -1
    fi
}

function checkZookeeperRole {
    is_job=`${dir}/kylin.sh io.kyligence.kap.tool.CuratorOperator $1 2>/dev/null`

    if [[ ${is_job} == "true" ]]; then
        quit "Failed, only one job node is allowed"
    fi
}

function quit {
        echo "$@"
        if [[ -n "${QUIT_MESSAGE_LOG}" ]]; then
            echo `setColor 31 "$@"` >> ${QUIT_MESSAGE_LOG}
        fi
        if [ $# == 2 ]
        then
            exit $2
        else
            exit 1
        fi
    }

function prepareFairScheduler() {
    cat > ${KYLIN_HOME}/conf/fairscheduler.xml <<EOL
<?xml version="1.0"?>

<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

<allocations>
  <pool name="query_pushdown">
    <schedulingMode>FAIR</schedulingMode>
    <weight>1</weight>
    <minShare>1</minShare>
  </pool>
  <pool name="extreme_heavy_tasks">
    <schedulingMode>FAIR</schedulingMode>
    <weight>3</weight>
    <minShare>1</minShare>
  </pool>
  <pool name="heavy_tasks">
    <schedulingMode>FAIR</schedulingMode>
    <weight>5</weight>
    <minShare>1</minShare>
  </pool>
  <pool name="lightweight_tasks">
    <schedulingMode>FAIR</schedulingMode>
    <weight>10</weight>
    <minShare>1</minShare>
  </pool>
  <pool name="vip_tasks">
    <schedulingMode>FAIR</schedulingMode>
    <weight>15</weight>
    <minShare>1</minShare>
  </pool>
</allocations>

EOL
}

function runTool() {
    prepareEnv

    java ${KYLIN_EXTRA_START_OPTS} -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} -Dhdp.version=current -cp "${kylin_hadoop_conf_dir}:${KYLIN_HOME}/lib/ext/*:${KYLIN_HOME}/tool/kap-tool-$version.jar:${SPARK_HOME}/jars/*" $@
}

function killChildProcess {
    if [ -f "${KYLIN_HOME}/child_process" ]
    then
        while childPid='' read -r line || [[ -n "$line" ]]; do
            # only kill orphan processes and spark-submit processes
            parentId=`ps -o ppid=, "$line"`
            for i in {1..5}
            do
              if ps -p $line > /dev/null
              then
                if [ "$parentId" -eq 1 ] && ps aux | grep $line | grep spark-submit > /dev/null
                then
                    verbose "Killing child process $line"
                    pkill -P $line
                else
                    sleep 1
                fi
                continue
              fi
              break
            done
        done < "${KYLIN_HOME}/child_process"
        rm -f ${KYLIN_HOME}/child_process
    fi
}

# start command
if [[ "$1" == io.kyligence.* ]]
then
    runTool "$@"
elif [ "$1" == "start" ]
then
    if [ -f "${KYLIN_HOME}/pid" ]
    then
        PID=`cat ${KYLIN_HOME}/pid`
    if ps -p $PID > /dev/null
        then
          quit "Kylin is running, stop it first, PID is $PID"
        fi
    fi

    killChildProcess

    prepareEnv

    cd ${KYLIN_HOME}/server
    source ${KYLIN_HOME}/bin/load-zookeeper-config.sh
    fetchFIZkInfo
    prepareFairScheduler

    serverMode=`$KYLIN_HOME/bin/get-properties.sh kylin.server.mode`
    if [ "$serverMode" == "job" ]; then
        echo "kylin.server.mode should be \"all\" or \"query\""
        exit -1
    fi

    checkRestPort
    checkZookeeperRole

    ${dir}/check-env.sh "if-not-yet" || exit 1

    java ${KYLIN_EXTRA_START_OPTS} -Dlogging.path=${KYLIN_HOME}/logs -Dspring.profiles.active=prod -Dlogging.config=file:${KYLIN_HOME}/conf/kylin-server-log4j.properties -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} -Dhdp.version=current -Dloader.path="${kylin_hadoop_conf_dir},${KYLIN_HOME}/lib/ext,${KYLIN_HOME}/server/jars,${SPARK_HOME}/jars" -XX:OnOutOfMemoryError="sh ${KYLIN_HOME}/bin/kylin.sh stop"  -jar newten.jar >> ${KYLIN_HOME}/logs/kylin.out 2>&1 & echo $! > ${KYLIN_HOME}/pid &
    PID=`cat ${KYLIN_HOME}/pid`
    CUR_DATE=$(date "+%Y-%m-%d %H:%M:%S")
    echo $CUR_DATE" new KE process pid is "$PID >> ${KYLIN_HOME}/logs/kylin.log

    echo "Kylin is starting, PID:`cat ${KYLIN_HOME}/pid`. Please checkout http://`hostname`:$port/kylin/index.html"

# stop command
elif [ "$1" == "stop" ]
then

    if [ -f "${KYLIN_HOME}/pid" ]
    then
        PID=`cat ${KYLIN_HOME}/pid`
        if ps -p $PID > /dev/null
        then
           echo "Stopping Kylin: $PID"
           kill $PID
           for i in {1..10}
           do
              sleep 3
              if ps -p $PID -f | grep kylin > /dev/null
              then
                 if [ "$i" == "10" ]
                 then
                    echo "Killing Kylin: $PID"
                    kill -9 $PID
                 fi
                 continue
              fi
              break
           done
           rm ${KYLIN_HOME}/pid

           killChildProcess

           exit 0
        else
           quit "Kylin is not running"
        fi

    else
        quit "Kylin is not running"
    fi

else
    quit "Usage: 'kylin.sh [-v] start' or 'kylin.sh [-v] stop'"
fi