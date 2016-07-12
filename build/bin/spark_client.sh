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
    
    bin/spark-submit --class org.apache.kylin.common.util.SparkEntry \
    --master yarn --deploy-mode client \
    $KYLIN_HOME/lib/kap-storage-parquet-1.5.3-SNAPSHOT.jar \
    -className io.kyligence.kap.storage.parquet.cube.spark.SparkQueryDriver  >> ${KYLIN_HOME}/logs/kylin.out 2>&1 & echo $! > ${KYLIN_HOME}/pid &
    

    #The location of all hadoop/hbase configurations are difficult to get.
    #Plus, some of the system properties are secretly set in hadoop/hbase shell command.
    #For example, in hdp 2.2, there is a system property called hdp.version,
    #which we cannot get until running hbase or hadoop shell command.
    #
    #To save all these troubles, we use hbase runjar to start tomcat.
    #In this way we no longer need to explicitly configure hadoop/hbase related classpath for tomcat,
    #hbase command will do all the dirty tasks for us:

    spring_profile=`sh ${dir}/get-properties.sh kylin.security.profile`
    if [ -z "$spring_profile" ]
    then
        echo 'please set kylin.security.profile in kylin.properties, options are: testing, ldap, saml.'
        exit 1
    else
        echo "kylin.security.profile is set to $spring_profile"
    fi

    #retrive $hive_dependency and $hbase_dependency
    source ${dir}/find-hive-dependency.sh
    source ${dir}/find-hbase-dependency.sh
    #retrive $KYLIN_EXTRA_START_OPTS
    if [ -f "${dir}/setenv.sh" ]
        then source ${dir}/setenv.sh
    fi

    export HBASE_CLASSPATH_PREFIX=${tomcat_root}/bin/bootstrap.jar:${tomcat_root}/bin/tomcat-juli.jar:${tomcat_root}/lib/*:$HBASE_CLASSPATH_PREFIX
    mkdir -p ${KYLIN_HOME}/ext
    export HBASE_CLASSPATH=$hive_dependency:${KYLIN_HOME}/lib/*:${KYLIN_HOME}/ext/*:${HBASE_CLASSPATH}:${KYLIN_HOME}/conf

    if [ -z "$KYLIN_REST_ADDRESS" ]
    then
        kylin_rest_address=`hostname -f`":"`grep "<Connector port=" ${tomcat_root}/conf/server.xml |grep protocol=\"HTTP/1.1\" | cut -d '=' -f 2 | cut -d \" -f 2`
        echo "KYLIN_REST_ADDRESS not found, will use ${kylin_rest_address}"
    else
        echo "KYLIN_REST_ADDRESS is set to: $KYLIN_REST_ADDRESS"
        kylin_rest_address=$KYLIN_REST_ADDRESS
    fi

    #debug if encounter NoClassDefError
    #hbase classpath

    # KYLIN_EXTRA_START_OPTS is for customized settings, checkout bin/setenv.sh
    hbase ${KYLIN_EXTRA_START_OPTS} \
    -Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager \
    -Dlog4j.configuration=kylin-server-log4j.properties \
    -Dorg.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH=true \
    -Dorg.apache.catalina.connector.CoyoteAdapter.ALLOW_BACKSLASH=true \
    -Djava.endorsed.dirs=${tomcat_root}/endorsed  \
    -Dcatalina.base=${tomcat_root} \
    -Dcatalina.home=${tomcat_root} \
    -Djava.io.tmpdir=${tomcat_root}/temp  \
    -Dkylin.hive.dependency=${hive_dependency} \
    -Dkylin.hbase.dependency=${hbase_dependency} \
    -Dkylin.rest.address=${kylin_rest_address} \
    -Dspring.profiles.active=${spring_profile} \
    org.apache.hadoop.util.RunJar ${tomcat_root}/bin/bootstrap.jar  org.apache.catalina.startup.Bootstrap start >> ${KYLIN_HOME}/logs/kylin.out 2>&1 & echo $! > ${KYLIN_HOME}/pid &
    echo "A new Kylin instance is started by $USER, stop it using \"kylin.sh stop\""
    echo "Please visit http://<ip>:7070/kylin"
    echo "You can check the log at ${KYLIN_HOME}/logs/kylin.log"
    exit 0

# stop command
elif [ "$1" == "stop" ]
then
    if [ -f "${KYLIN_HOME}/pid" ]
    then
        PID=`cat $KYLIN_HOME/pid`
        if ps -p $PID > /dev/null
        then
           echo "stopping Kylin:$PID"
           kill $PID
           rm ${KYLIN_HOME}/pid
           exit 0
        else
           echo "Kylin is not running, please check"
           exit 1
        fi
        
    else
        echo "Kylin is not running, please check"
        exit 1    
    fi

# streaming command
elif [ "$1" == "streaming" ]
then
    if [ $# -lt 4 ]
    then
        echo "invalid input args $@"
        exit -1
    fi
    if [ "$2" == "start" ]
    then

        #retrive $hive_dependency and $hbase_dependency
        source ${dir}/find-hive-dependency.sh
        source ${dir}/find-hbase-dependency.sh
        #retrive $KYLIN_EXTRA_START_OPTS
        if [ -f "${dir}/setenv.sh" ]
            then source ${dir}/setenv.sh
        fi

        mkdir -p ${KYLIN_HOME}/ext
        export HBASE_CLASSPATH=$hive_dependency:${KYLIN_HOME}/lib/*:${KYLIN_HOME}/ext/*:${HBASE_CLASSPATH}

        # KYLIN_EXTRA_START_OPTS is for customized settings, checkout bin/setenv.sh
        hbase ${KYLIN_EXTRA_START_OPTS} \
        -Dlog4j.configuration=kylin-log4j.properties\
        -Dkylin.hive.dependency=${hive_dependency} \
        -Dkylin.hbase.dependency=${hbase_dependency} \
        org.apache.kylin.engine.streaming.cli.StreamingCLI $@ > ${KYLIN_HOME}/logs/streaming_$3_$4.log 2>&1 & echo $! > ${KYLIN_HOME}/logs/$3_$4 &
        echo "streaming started name: $3 id: $4"
        exit 0
    elif [ "$2" == "stop" ]
    then
        if [ ! -f "${KYLIN_HOME}/$3_$4" ]
            then
                echo "streaming is not running, please check"
                exit 1
            fi
            pid=`cat ${KYLIN_HOME}/$3_$4`
            if [ "$pid" = "" ]
            then
                echo "streaming is not running, please check"
                exit 1
            else
                echo "stopping streaming:$pid"
                kill $pid
            fi
            rm ${KYLIN_HOME}/$3_$4
            exit 0
        else
            echo
        fi

# monitor command
elif [ "$1" == "monitor" ]
then
    echo "monitor job"

    #retrive $hive_dependency and $hbase_dependency
    source ${dir}/find-hive-dependency.sh
    source ${dir}/find-hbase-dependency.sh
    #retrive $KYLIN_EXTRA_START_OPTS
    if [ -f "${dir}/setenv.sh" ]
        then source ${dir}/setenv.sh
    fi

    mkdir -p ${KYLIN_HOME}/ext
    export HBASE_CLASSPATH=$hive_dependency:${KYLIN_HOME}/lib/*:${KYLIN_HOME}/ext/*:${HBASE_CLASSPATH}

    # KYLIN_EXTRA_START_OPTS is for customized settings, checkout bin/setenv.sh
    hbase ${KYLIN_EXTRA_START_OPTS} \
    -Dlog4j.configuration=kylin-log4j.properties\
    -Dkylin.hive.dependency=${hive_dependency} \
    -Dkylin.hbase.dependency=${hbase_dependency} \
    org.apache.kylin.engine.streaming.cli.MonitorCLI $@ > ${KYLIN_HOME}/logs/monitor.log 2>&1
    exit 0

elif [ "$1" = "version" ]
then
    export HBASE_CLASSPATH=${KYLIN_HOME}/lib/*
    exec hbase ${KYLIN_EXTRA_START_OPTS} -Dlog4j.configuration=kylin-log4j.properties org.apache.kylin.common.KylinVersion
    exit 0

elif [ "$1" = "diag" ]
then
    shift
    exec $KYLIN_HOME/bin/diag.sh $@
    exit 0

# tool command
elif [[ "$1" = org.apache.kylin.* ]] || [[ "$1" = io.kyligence.* ]]
then
    #retrive $hive_dependency and $hbase_dependency
    source ${dir}/find-hive-dependency.sh
    source ${dir}/find-hbase-dependency.sh
    #retrive $KYLIN_EXTRA_START_OPTS
    if [ -f "${dir}/setenv-tool.sh" ]
        then source ${dir}/setenv-tool.sh
    fi

    export HBASE_CLASSPATH=${KYLIN_HOME}/lib/*:${KYLIN_HOME}/tool/*:$hive_dependency:${HBASE_CLASSPATH}

    exec hbase ${KYLIN_EXTRA_START_OPTS} -Dkylin.hive.dependency=${hive_dependency} -Dkylin.hbase.dependency=${hbase_dependency} -Dlog4j.configuration=kylin-log4j.properties "$@"

else
    echo "usage: kylin.sh start or kylin.sh stop"
    exit 1
fi
