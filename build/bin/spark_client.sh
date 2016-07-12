#!/bin/bash
# Kyligence Inc. License

dir=$(dirname ${0})

# We should set KYLIN_HOME here for multiple tomcat instsances that are on the same node.
# In addition, we should set a KYLIN_HOME for the global use as normal.
export KYLIN_HOME=${dir}/../
mkdir -p ${KYLIN_HOME}/logs

if [ -z "$SPARK_HOME" ]
then
    echo 'please make sure SPARK_HOME has been set'
    exit 1
else
    echo "SPARK_HOME is set to ${SPARK_HOME}"
fi


# start command
if [ "$1" == "start" ]
then
    if [ -f "${KYLIN_HOME}/spark_client_pid" ]
    then
        PID=`cat $KYLIN_HOME/spark_client_pid`
        if ps -p $PID > /dev/null
        then
          echo "Spark Client is running, stop it first"
          exit 1
        fi
    fi
    
    $SPARK_HOME/bin/spark-submit --class org.apache.kylin.common.util.SparkEntry \
    --master yarn --deploy-mode client $KYLIN_HOME/lib/kap-storage-parquet-1.5.3-SNAPSHOT.jar \
    -className io.kyligence.kap.storage.parquet.cube.spark.SparkQueryDriver  >> ${KYLIN_HOME}/logs/spark_client.out 2>&1 & echo $! > ${KYLIN_HOME}/spark_client_pid &
    
    echo "A new spark client instance is started by $USER, stop it using \"spark_client.sh stop\""
    echo "You can check the log at ${KYLIN_HOME}/logs/spark_client.out"
    exit 0

# stop command
elif [ "$1" == "stop" ]
then
    if [ -f "${KYLIN_HOME}/spark_client_pid" ]
    then
        PID=`cat $KYLIN_HOME/spark_client_pid`
        if ps -p $PID > /dev/null
        then
           echo "stopping Spark client:$PID"
           kill $PID
           rm ${KYLIN_HOME}/spark_client
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
