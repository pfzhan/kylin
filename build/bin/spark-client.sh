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
    echo "Starting test spark with conf"

    retrieveSparkEnvProps
    echo "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"

    mkdir -p ${KYLIN_HOME}/var
    input_file=${KYLIN_HOME}/var/spark_client_test_input
    [[ ! -f ${input_file} ]] || rm -f ${input_file}
    echo "Hello Spark Client" >> ${input_file};
    source ${dir}/hdfs-op.sh put ${input_file}

    submitCommand='$SPARK_HOME/bin/spark-submit --class io.kyligence.kap.tool.setup.KapSparkTaskTestCLI --name Test  $KYLIN_SPARK_TEST_JAR_PATH ${TARGET_HDFS_FILE} '
    submitCommand=${submitCommand}${confStr}
    verbose "The submit command is: $submitCommand"
    eval $submitCommand
    if [ $? == 0 ];then
        ${dir}/hdfs-op.sh rm ${input_file}
    else
        ${dir}/hdfs-op.sh rm ${input_file}
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
        cat << EOF > $SPARK_HOME/conf/hive-site.xml
        <configuration>
            <property>
                <name>hive.metastore.uris</name>
                <value>thrift://sandbox.hortonworks.com:9083</value>
            </property>
        </configuration>
EOF
    fi

    if [ -f "${KYLIN_HOME}/spark_client_pid" ]
    then
        PID=`cat $KYLIN_HOME/spark_client_pid`
        if ps -p $PID > /dev/null
        then
          quit "Spark Client is running, stop it first"
        fi
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

    echo "A new Spark Client instance is started by $USER. To stop it, run 'spark_client.sh stop'"
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
    if [[ $CI_MODE == 'true' ]]
    then
        rm ${SPARK_HOME}/conf/hive-site.xml
    fi

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
