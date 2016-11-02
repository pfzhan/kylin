#!/bin/bash
# Kyligence Inc. License

dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

if [[ $CI_MODE == 'true' ]]
then
    echo 'in ci mode'
    export KYLIN_HOME=${dir}/../../
    export CONF_DIR=${KYLIN_HOME}/extensions/examples/test_case_data/sandbox
    export SPARK_DIR=${KYLIN_HOME}/build/spark/
    export KYLIN_SPARK_JAR_PATH=`ls $KYLIN_HOME/extensions/storage-parquet/target/kap-storage-parquet-*-spark.jar`
else
    echo 'in normal mode'
    export KYLIN_HOME=${KYLIN_HOME:-"${dir}/../"}
    export CONF_DIR=${KYLIN_HOME}/conf
    export SPARK_DIR=${KYLIN_HOME}/spark/
    export KYLIN_SPARK_JAR_PATH=`ls $KYLIN_HOME/lib/kylin-storage-parquet-kap-*.jar`
    
    if [ ! -f ${KYLIN_HOME}/commit_SHA1 ]
    then
        echo "Seems you're not in binary package, did you forget to set CI_MODE=true?"
        exit 1
    fi
fi

echo "KYLIN_HOME is set to ${KYLIN_HOME}"
echo "CONF_DIR is set to ${CONF_DIR}"
echo "SPARK_DIR is set to ${SPARK_DIR}"
echo "KYLIN_SPARK_JAR_PATH is set to ${KYLIN_SPARK_JAR_PATH}"

mkdir -p ${KYLIN_HOME}/logs

#auto detect SPARK_HOME
if [ -z "$SPARK_HOME" ]
then
    if [ -d ${SPARK_DIR} ]
    then
        export SPARK_HOME=${SPARK_DIR}
        echo "SPARK_HOME is set to ${SPARK_HOME}"
    else
        echo 'please make sure SPARK_HOME has been set(export as environment variable first)'
        exit 1
    fi
else
    echo "SPARK_HOME is set to ${SPARK_HOME}"
fi

# start command
if [ "$1" == "start" ] # ./spark_client.sh start [port]
then
    echo "Starting the spark driver"
    if [ -f "${KYLIN_HOME}/spark_client_pid" ]
    then
        PID=`cat $KYLIN_HOME/spark_client_pid`
        if ps -p $PID > /dev/null
        then
          echo "Spark Client is running, stop it first"
          exit 1
        fi
    fi
    
    #spark driver client port
    driverPortPrefix="kap.storage.columnar.spark.driver.port="
    realStart=$((${#driverPortPrefix} + 1))
    driverPort=
    for x in `cat ${CONF_DIR}/*.properties | grep "^$driverPortPrefix" | cut -c ${realStart}-   `
    do  
        driverPort=$x
    done
    
    driverPort=${driverPort:-7071}
    echo "The driver port is $driverPort"
    export driverPort

    nc -z -w 5 localhost ${driverPort} 1>/dev/null 2>&1; nc_result=$?
    if [ $nc_result -eq 0 ]; then
        echo "port ${driverPort} is not available, could not start Spark Driver"
        exit 1
    else
        echo "port ${driverPort} is available"
    fi

    # spark envs
    sparkEnvPrefix="kap.storage.columnar.env."
    realStart=$((${#sparkEnvPrefix} + 1))
    for kv in `cat ${CONF_DIR}/*.properties | grep "^${sparkEnvPrefix}" | cut -c ${realStart}-   `
    do  
        key=`echo "$kv" |  awk '{ n = index($0,"="); print substr($0,0,n-1)}'`
        existingValue=`printenv ${key}`
        if [ -z "$existingValue" ] 
        then
            echo "$key is not set, running: export $kv"
            export $kv
        else
            echo "$key already has value: $existingValue, use it"
        fi
    done
    
    # spark conf
    sparkConfPrefix="kap.storage.columnar.conf."
    realStart=$((${#sparkConfPrefix} + 1))
    confStr=`cat ${CONF_DIR}/*.properties | grep "^${sparkConfPrefix}" | cut -c ${realStart}- |  awk '{ print "--conf " "\"" $0 "\""}' | tr '\n' ' ' `
    echo "additional confs spark-submit: $confStr"

    submitCommand='$SPARK_HOME/bin/spark-submit --class org.apache.kylin.common.util.SparkEntry --master yarn --deploy-mode client --verbose '
    submitCommand=${submitCommand}${confStr}
    submitCommand=${submitCommand}' ${KYLIN_SPARK_JAR_PATH} -className io.kyligence.kap.storage.parquet.cube.spark.SparkQueryDriver --port ${driverPort} >> ${KYLIN_HOME}/logs/spark_client.out 2>&1 & echo $! > ${KYLIN_HOME}/spark_client_pid &'
    #echo "The submit command is: $submitCommand"
    eval $submitCommand 
    
    echo "A new spark client instance is started by $USER, stop it using \"spark_client.sh stop\""
    echo "You can check the log at ${KYLIN_HOME}/logs/spark_client.out"
    
    if [[ $CI_MODE == 'true' ]]
    then
        echo "sleep one minute before exit, allowing spark fully start"
        sleep 60
    fi
    
    exit 0

# stop command
elif [ "$1" == "stop" ]
then
    echo "Stopping the spark driver"
    if [ -f "${KYLIN_HOME}/spark_client_pid" ]
    then
        PID=`cat $KYLIN_HOME/spark_client_pid`
        if ps -p $PID > /dev/null
        then
           echo "stopping Spark client:$PID"
           kill $PID
           rm ${KYLIN_HOME}/spark_client_pid
           exit 0
        else
           echo "Spark Client is not running, please check"
           exit 1
        fi
        
    else
        echo "Spark Client is not running, please check"
        exit 1    
    fi
else
    echo "usage: spark_client.sh start or spark_clint.sh stop"
    exit 1
fi
