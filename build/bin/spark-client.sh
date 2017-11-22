#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh $@

if [ "$1" == "-v" ]; then
    shift
fi


if [[ $CI_MODE == 'true' ]]
then
    verbose 'in ci mode'
    export KYLIN_HOME=`cd "${KYLIN_HOME}/.."; pwd`
    export CONF_DIR=${KYLIN_HOME}/extensions/examples/test_case_data/sandbox
    export KYLIN_CONF=$CONF_DIR
    export LOG4J_DIR=${KYLIN_HOME}/build/conf
    export SPARK_DIR=${KYLIN_HOME}/build/spark/
    export KYLIN_SPARK_TEST_JAR_PATH=`ls $KYLIN_HOME/extensions/tool-assembly/target/kap-tool-assembly-*.jar`
    export KYLIN_SPARK_JAR_PATH=`ls $KYLIN_HOME/extensions/storage-parquet/target/kap-storage-parquet-*-spark.jar`
    export KAP_HDFS_WORKING_DIR=`$KYLIN_HOME/build/bin/get-properties.sh kylin.env.hdfs-working-dir`
    export KAP_METADATA_URL=`$KYLIN_HOME/build/bin/get-properties.sh kylin.metadata.url`
    export SPARK_ENV_PROPS=`$KYLIN_HOME/build/bin/get-properties.sh kap.storage.columnar.spark-env.`
    export SPARK_CONF_PROPS=`$KYLIN_HOME/build/bin/get-properties.sh kap.storage.columnar.spark-conf.`
    export SPARK_DRIVER_PORT=`$KYLIN_HOME/build/bin/get-properties.sh kap.storage.columnar.spark-driver-port`
else
    verbose 'in normal mode'
    export KYLIN_HOME=${KYLIN_HOME:-"${dir}/../"}
    export CONF_DIR=${KYLIN_HOME}/conf
    export LOG4J_DIR=${KYLIN_HOME}/conf
    export SPARK_DIR=${KYLIN_HOME}/spark/
    export KYLIN_SPARK_TEST_JAR_PATH=`ls $KYLIN_HOME/tool/kylin-tool-kap-*.jar`
    export KYLIN_SPARK_JAR_PATH=`ls $KYLIN_HOME/lib/kylin-storage-parquet-kap-*.jar`
    export KAP_HDFS_WORKING_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.env.hdfs-working-dir`
    export KAP_METADATA_URL=`$KYLIN_HOME/bin/get-properties.sh kylin.metadata.url`
    export SPARK_ENV_PROPS=`$KYLIN_HOME/bin/get-properties.sh kap.storage.columnar.spark-env.`
    export SPARK_CONF_PROPS=`$KYLIN_HOME/bin/get-properties.sh kap.storage.columnar.spark-conf.`
    export SPARK_DRIVER_PORT=`$KYLIN_HOME/bin/get-properties.sh kap.storage.columnar.spark-driver-port`

    if [ ! -f ${KYLIN_HOME}/commit_SHA1 ]
    then
        quit "Seems you're not in binary package, did you forget to set CI_MODE=true?"
    fi
fi

source ${dir}/find-hadoop-conf-dir.sh
export KAP_SPARK_IDENTIFIER=$RANDOM
export KAP_HDFS_APPENDER_JAR=`basename ${KYLIN_SPARK_JAR_PATH}`

# get local ip for htrace-zipkin use
if [ -z "$ZIPKIN_HOSTNAME" ]
then
    export ZIPKIN_HOSTNAME=`hostname`
fi
echo "ZIPKIN_HOSTNAME is set to ${ZIPKIN_HOSTNAME}"
echo "ZIPKIN_SCRIBE_PORT is set to ${ZIPKIN_SCRIBE_PORT}"

verbose "KYLIN_HOME is set to ${KYLIN_HOME}"
verbose "CONF_DIR is set to ${CONF_DIR}"
verbose "SPARK_DIR is set to ${SPARK_DIR}"
verbose "KYLIN_SPARK_JAR_PATH is set to ${KYLIN_SPARK_JAR_PATH}"

mkdir -p ${KYLIN_HOME}/logs

#auto detect SPARK_HOME
if [ -z "$SPARK_HOME" ]
then
    if [ -d ${SPARK_DIR} ]
    then
        export SPARK_HOME=${SPARK_DIR}
    else
        quit 'Please make sure SPARK_HOME has been set (export as environment variable first)'
    fi
fi
echo "SPARK_HOME is set to ${SPARK_HOME}"

function retrieveSparkEnvProps()
{
 # spark envs
    for kv in `echo "$SPARK_ENV_PROPS"`
    do
        key=`echo "$kv" |  awk '{ n = index($0,"="); print substr($0,0,n-1)}'`
        existingValue=`printenv ${key}`
        if [ -z "$existingValue" ]
        then
            verbose "export" `eval "verbose $kv"`
            eval "export $kv"
        else
            verbose "$key already has value: $existingValue, use it"
        fi
    done

    # spark conf
    confStr=`echo "$SPARK_CONF_PROPS" |  awk '{ print "--conf " "\"" $0 "\""}' | tr '\n' ' ' `
    verbose "additional confs spark-submit: $confStr"
}

if [ "$1" == "test" ]
then
    source ${dir}/find-working-dir.sh
    echo "Starting test spark with conf"

    retrieveSparkEnvProps
    echo "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"

    local_input_dir=${KYLIN_HOME}/logs/tmp
    input_file=spark_client_test_input
    full_input_file=${local_input_dir}/${input_file}
    mkdir -p ${local_input_dir}

    [[ ! -f ${full_input_file} ]] || rm -f ${full_input_file}
    echo "Hello Spark Client" >> ${full_input_file};

    hadoop ${KAP_HADOOP_PARAM} fs -put -f ${full_input_file} ${KAP_WORKING_DIR}

    submitCommand='$SPARK_HOME/bin/spark-submit --class io.kyligence.kap.tool.setup.KapSparkTaskTestCLI --name Test  $KYLIN_SPARK_TEST_JAR_PATH ${KAP_WORKING_DIR}/${input_file} '
    submitCommand=${submitCommand}${confStr}
    verbose "The submit command is: $submitCommand"
    eval $submitCommand
    if [ $? == 0 ];then
        hadoop ${KAP_HADOOP_PARAM} fs -rm -r -skipTrash ${KAP_WORKING_DIR}/${input_file}
        rm -rf ${full_input_file}
    else
        hadoop ${KAP_HADOOP_PARAM} fs -rm -r -skipTrash ${KAP_WORKING_DIR}/${input_file}
        rm -rf ${full_input_file}
        quit "ERROR: error when testing spark with spark configurations in KAP!"
    fi
    exit 0
fi
# start command
if [ "$1" == "start" ] # ./spark-client.sh start [port]
then
    echo "Starting Spark Client..."

    if [[ $CI_MODE == 'true' ]]
    then
        HIVE_METASTORE_URI="thrift://sandbox.hortonworks.com:9083"
    else
        if [[ -z $HIVE_METASTORE_URI ]]
        then
         source ${dir}/find-hive-dependency.sh
        HIVE_METASTORE_URI=$(${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.mr.HadoopConfPropertyRetriever ${hive_conf_path}/hive-site.xml hive.metastore.uris | grep -v Retrieving | tail -1)
        fi
        if [[ -z "$HIVE_METASTORE_URI" ]]
        then
            quit "Couldn't find hive.metastore.uris in hive-site.xml. hive.metastore.uris specifies Thrift URI of hive metastore . Please export HIVE_METASTORE_URI with hive.metastore.uris before starting kylin , for example: export HIVE_METASTORE_URI=thrift://sandbox.hortonworks.com:9083"
        fi
    fi

    if [ -f "${KYLIN_HOME}/spark_client_pid" ]
    then
        PID=`cat $KYLIN_HOME/spark_client_pid`
        if ps -p $PID > /dev/null
        then
          quit "Spark Client is running, stop it first"
        fi
    fi
    if [ ! -e "${KYLIN_HOME}/conf/fairscheduler.xml" ]
    then
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
  <pool name="table_index">
    <schedulingMode>FAIR</schedulingMode>
    <weight>1</weight>
    <minShare>1</minShare>
  </pool>
  <pool name="cube">
    <schedulingMode>FAIR</schedulingMode>
    <weight>10</weight>
    <minShare>1</minShare>
  </pool>
    <pool name="query_pushdown">
    <schedulingMode>FAIR</schedulingMode>
    <weight>1</weight>
    <minShare>1</minShare>
  </pool>
</allocations>

EOL
    fi
    #Spark Client port
    driverPort=${SPARK_DRIVER_PORT:-7071}
    verbose "The driver port is $driverPort"
    export driverPort

    nc -z -w 5 localhost ${driverPort} 1>/dev/null 2>&1; nc_result=$?
    if [ $nc_result -eq 0 ]; then
        quit "Port ${driverPort} is not available, could not start Spark Client"
    fi
    retrieveSparkEnvProps
    submitCommand='$SPARK_HOME/bin/spark-submit --class org.apache.kylin.common.util.SparkEntry --master yarn --deploy-mode client --verbose --files "${LOG4J_DIR}/spark-executor-log4j.properties,${KYLIN_SPARK_JAR_PATH}" '
    submitCommand=${submitCommand}${confStr}
    submitCommand=${submitCommand}' ${KYLIN_SPARK_JAR_PATH} -className io.kyligence.kap.storage.parquet.cube.spark.SparkQueryDriver --port ${driverPort} > ${KYLIN_HOME}/logs/spark-driver.out 2>&1 & echo $! > ${KYLIN_HOME}/spark_client_pid &'
    verbose "The submit command is: $submitCommand"
    eval $submitCommand

    echo "A new Spark Client instance is started by $USER. To stop it, run 'spark-client.sh stop'"
    echo "Check the log at ${KYLIN_HOME}/logs/spark-driver.log"

    if [[ $CI_MODE == 'true' ]]
    then
        echo "sleep one minute before exit, allowing spark fully start"
        sleep 60
    fi

    exit 0

# stop command
elif [ "$1" == "stop" ]
then
    if [ -f "${KYLIN_HOME}/spark_client_pid" ]
    then
        PID=`cat $KYLIN_HOME/spark_client_pid`
        if ps -p $PID > /dev/null
        then
           echo "Stopping Spark Client: $PID"
           kill $PID
           rm ${KYLIN_HOME}/spark_client_pid
           exit 0
        else
           quit "Spark Client is not running"
        fi

    else
        quit "Spark client pid is not found"
    fi
else
    quit "Usage: 'spark-client.sh [-v] start' or 'spark-clint.sh [-v] stop'"
fi
