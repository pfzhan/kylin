#!/bin/bash
# Kyligence Inc. License

dir=$(dirname ${0})

if [ -z "$SPARK_HOME" ]
then
    echo 'please make sure SPARK_HOME has been set'
    exit 1
else
    echo "SPARK_HOME is set to ${SPARK_HOME}"
fi

if [[ $CI_MODE == 'true' ]]
then
    echo 'in ci mode'
    export KAP_HOME=${dir}/../../
    export KYLIN_SPARK_JAR_PATH=$KAP_HOME/extensions/storage-parquet/target/kap-storage-parquet-1.5.3-SNAPSHOT-spark.jar
else
    echo 'in normal mode'
    export KAP_HOME=${dir}/../
    export KYLIN_SPARK_JAR_PATH=$KAP_HOME/lib/kylin-storage-parquet-kap-1.5.3-SNAPSHOT.jar
fi

echo "KYLIN_HOME is set to ${KAP_HOME}"
echo "KYLIN_SPARK_JAR_PATH is set to ${KYLIN_SPARK_JAR_PATH}"

mkdir -p ${KAP_HOME}/logs

# start command
if [ "$1" == "start" ]
then
    echo "Starting the spark driver client"
    if [ -f "${KAP_HOME}/spark_client_pid" ]
    then
        PID=`cat $KAP_HOME/spark_client_pid`
        if ps -p $PID > /dev/null
        then
          echo "Spark Client is running, stop it first"
          exit 1
        fi
    fi
    
    $SPARK_HOME/bin/spark-submit --class org.apache.kylin.common.util.SparkEntry \
    --master yarn --deploy-mode client ${KYLIN_SPARK_JAR_PATH} \
    -className io.kyligence.kap.storage.parquet.cube.spark.SparkQueryDriver  >> ${KAP_HOME}/logs/spark_client.out 2>&1 & echo $! > ${KAP_HOME}/spark_client_pid &
    
    echo "A new spark client instance is started by $USER, stop it using \"spark_client.sh stop\""
    echo "You can check the log at ${KAP_HOME}/logs/spark_client.out"
    
    if [[ $CI_MODE == 'true' ]]
    then
        echo "sleep one minute before exit, allowing spark fully start"
        sleep 60
    fi
    
    exit 0

# stop command
elif [ "$1" == "stop" ]
then
    echo "Stopping the spark driver client"
    if [ -f "${KAP_HOME}/spark_client_pid" ]
    then
        PID=`cat $KAP_HOME/spark_client_pid`
        if ps -p $PID > /dev/null
        then
           echo "stopping Spark client:$PID"
           kill $PID
           rm ${KAP_HOME}/spark_client_pid
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
