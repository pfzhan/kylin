#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh $@

if [ "$verbose" = true ]; then
    shift
fi

if [[ $CI_MODE == 'true' ]]
then
    verbose 'in ci mode'
    export KYLIN_HOME=`cd "${KYLIN_HOME}/.."; pwd`
    export CONF_DIR=${KYLIN_HOME}/extensions/examples/test_case_data/sandbox
    export SPARK_DIR=${KYLIN_HOME}/build/spark/
    export KYLIN_SPARK_JAR_PATH=`ls $KYLIN_HOME/extensions/storage-parquet/target/kap-storage-parquet-*-spark.jar`
else
    verbose 'in normal mode'
    export KYLIN_HOME=${KYLIN_HOME:-"${dir}/../"}
    export CONF_DIR=${KYLIN_HOME}/conf
    export SPARK_DIR=${KYLIN_HOME}/spark/
    export KYLIN_SPARK_JAR_PATH=`ls $KYLIN_HOME/lib/kylin-storage-parquet-kap-*.jar`
    
    if [ ! -f ${KYLIN_HOME}/commit_SHA1 ]
    then
        quit "Seems you're not in binary package, did you forget to set CI_MODE=true?"
    fi
fi

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

# start command
if [ "$1" == "start" ] # ./spark_client.sh start [port]
then
    echo "Starting Spark Client..."

    if [ -f "${KYLIN_HOME}/spark_client_pid" ]
    then
        PID=`cat $KYLIN_HOME/spark_client_pid`
        if ps -p $PID > /dev/null
        then
          quit "Spark Client is running, stop it first"
        fi
    fi
    
    #Spark Client port
    driverPortPrefix="kap.storage.columnar.spark.driver.port="
    realStart=$((${#driverPortPrefix} + 1))
    driverPort=
    for x in `cat ${CONF_DIR}/*.properties | grep "^$driverPortPrefix" | cut -c ${realStart}-   `
    do  
        driverPort=$x
    done
    
    driverPort=${driverPort:-7071}
    verbose "The driver port is $driverPort"
    export driverPort

    nc -z -w 5 localhost ${driverPort} 1>/dev/null 2>&1; nc_result=$?
    if [ $nc_result -eq 0 ]; then
        quit "Port ${driverPort} is not available, could not start Spark Client"
    fi
    
    if [ ! -f ${SPARK_HOME}/conf/log4j.properties ] ; then
        echo "${SPARK_HOME}/conf/log4j.properties does not exist, create one:"
        cat << EOF > ${SPARK_HOME}/conf/log4j.properties 
log4j.rootLogger=INFO,stdout

log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{ISO8601} %-5p [%t %c{1}:%L]: %m%n

#log4j.logger.org.apache.hadoop=ERROR
log4j.logger.org.apache.kylin=DEBUG
log4j.logger.io.kyligence=DEBUG
log4j.logger.org.springframework=WARN
EOF
    else
        echo "${SPARK_HOME}/conf/log4j.properties already exists"
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
            verbose "$key is not set, running: export $kv"
            export $kv
        else
            verbose "$key already has value: $existingValue, use it"
        fi
    done
    
    # spark conf
    sparkConfPrefix="kap.storage.columnar.conf."
    realStart=$((${#sparkConfPrefix} + 1))
    confStr=`cat ${CONF_DIR}/*.properties | grep "^${sparkConfPrefix}" | cut -c ${realStart}- |  awk '{ print "--conf " "\"" $0 "\""}' | tr '\n' ' ' `
    verbose "additional confs spark-submit: $confStr"

    submitCommand='$SPARK_HOME/bin/spark-submit --class org.apache.kylin.common.util.SparkEntry --master yarn --deploy-mode client --verbose --files ${SPARK_HOME}/conf/log4j.properties '
    submitCommand=${submitCommand}${confStr}
    submitCommand=${submitCommand}' ${KYLIN_SPARK_JAR_PATH} -className io.kyligence.kap.storage.parquet.cube.spark.SparkQueryDriver --port ${driverPort} >> ${KYLIN_HOME}/logs/spark_client.out 2>&1 & echo $! > ${KYLIN_HOME}/spark_client_pid &'
    verbose "The submit command is: $submitCommand"
    eval $submitCommand 
    
    echo "A new Spark Client instance is started by $USER. To stop it, run 'spark_client.sh stop'"
    echo "Check the log at ${KYLIN_HOME}/logs/spark_client.out"
    
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
        quit "Spark Client is not running"
    fi
else
    quit "Usage: 'spark_client.sh [-v] start' or 'spark_clint.sh [-v] stop'"
fi
