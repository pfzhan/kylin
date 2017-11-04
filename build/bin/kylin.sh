#!/bin/bash
# Kyligence Inc. License


# set verbose=true to print more logs in scripts
source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh $@
if [ "$1" == "-v" ]; then
    shift
fi

function retrieveDependency() {

    # get hive_dependency
    if [ -z "${hive_dependency}" ]; then
	    source ${dir}/find-hive-dependency.sh
    fi

    # get spark_dependency
    if [ -z "${spark_dependency}" ]; then
            source ${dir}/find-spark-dependency.sh
    fi

    # get hbase_dependency
    if [ -z "${hbase_dependency}" ]; then
        metadataUrl=`${dir}/get-properties.sh kylin.metadata.url`
        storage_type=${metadataUrl#*@}
        storage_type=${storage_type%%,*}
        if [[ "${storage_type}" == "hbase" ]]
        then
            source ${dir}/find-hbase-dependency.sh
        fi
    fi
    
    # get kafka_dependency
    if [ -n "${KAFKA_HOME}" ]; then
        source ${dir}/find-kafka-dependency.sh
    fi

    # get kylin_hadoop_conf_dir & kylin_hadoop_opts
    if [ -z "${kylin_hadoop_conf_dir}" ]; then
        source ${dir}/find-hadoop-conf-dir.sh
    fi

    tomcat_root=${dir}/../tomcat

    # get KYLIN_REST_ADDRESS
    if [ -z "$KYLIN_REST_ADDRESS" ]
    then
        KYLIN_REST_ADDRESS=`hostname -f`":"`grep "<Connector port=" ${tomcat_root}/conf/server.xml |grep protocol=\"HTTP/1.1\" | cut -d '=' -f 2 | cut -d \" -f 2`
        export KYLIN_REST_ADDRESS
        verbose "KYLIN_REST_ADDRESS is ${KYLIN_REST_ADDRESS}"
    fi

    # compose hadoop_dependencies
    hadoop_dependencies=${kylin_hadoop_conf_dir}
    if [ -n "${hbase_dependency}" ]; then
        hadoop_dependencies=${hadoop_dependencies}:${hbase_dependency}
    else
        hadoop_dependencies=${hadoop_dependencies}:`hadoop classpath`   || quit "Command 'hadoop classpath' does not work. Please check hadoop is installed correctly."
    fi
    if [ -n "${hive_dependency}" ]; then
        hadoop_dependencies=${hadoop_dependencies}:${hive_dependency}
    fi
    if [ -n "${kafka_dependency}" ]; then
        hadoop_dependencies=${hadoop_dependencies}:${kafka_dependency}
    fi
    if [ -n "${spark_dependency}" ]; then
        hadoop_dependencies=${hadoop_dependencies}:${spark_dependency}
    fi
    
    # compose KYLIN_TOMCAT_CLASSPATH
    tomcat_classpath=${tomcat_root}/bin/bootstrap.jar:${tomcat_root}/bin/tomcat-juli.jar:${tomcat_root}/lib/*
    export KYLIN_TOMCAT_CLASSPATH=${tomcat_classpath}:${KYLIN_HOME}/conf:${KYLIN_HOME}/lib/*:${KYLIN_HOME}/ext/*:${hadoop_dependencies}
    
    # compose KYLIN_TOOL_CLASSPATH
    export KYLIN_TOOL_CLASSPATH=${KYLIN_HOME}/conf:${KYLIN_HOME}/tool/*:${KYLIN_HOME}/ext/*:${hadoop_dependencies}
    
    # compose kylin_common_opts
    kylin_common_opts="${kylin_hadoop_opts} \
    -Dkylin.hive.dependency=${hive_dependency} \
    -Dkylin.kafka.dependency=${kafka_dependency} \
    -Dkylin.spark.dependency=${spark_dependency} \
    -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} \
    -Dkap.server.host-address=${KYLIN_REST_ADDRESS} \
    -Dspring.profiles.active=${spring_profile}"
    
    # compose KYLIN_TOMCAT_OPTS
    KYLIN_TOMCAT_OPTS="-Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-server-log4j.properties \
    -Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager \
    -Dorg.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH=true \
    -Dorg.apache.catalina.connector.CoyoteAdapter.ALLOW_BACKSLASH=true \
    -Djava.endorsed.dirs=${tomcat_root}/endorsed  \
    -Dcatalina.base=${tomcat_root} \
    -Dcatalina.home=${tomcat_root} \
    -Djava.io.tmpdir=${tomcat_root}/temp ${kylin_common_opts}"
    export KYLIN_TOMCAT_OPTS
    
    # compose KYLIN_TOOL_OPTS
    KYLIN_TOOL_OPTS="-Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties ${kylin_common_opts}"
    export KYLIN_TOOL_OPTS
}

function checkBasicKylinProps() {
    fileVersion=`cat ${KYLIN_HOME}/VERSION`

    spring_profile=`${dir}/get-properties.sh kylin.security.profile`
    if [ -z "$spring_profile" ]
    then
        quit 'Please set kylin.security.profile in kylin.properties, options are: testing, ldap, saml.'
    else
        verbose "kylin.security.profile is $spring_profile"
    fi
}

function checkRestPort() {
    kylin_rest_address_arr=(${KYLIN_REST_ADDRESS//:/ })
    inuse=`netstat -tlpn | grep "\b${kylin_rest_address_arr[1]}\b"`
    [[ -z ${inuse} ]] || quit "Port ${kylin_rest_address_arr[1]} is not available. Another KAP server is running?"
}

function classpathDebug() {
    if [ "${KYLIN_CLASSPATH_DEBUG}" != "" ]; then
        echo "Finding ${KYLIN_CLASSPATH_DEBUG} on classpath" $@
        $JAVA -classpath $@ org.apache.kylin.common.util.ClasspathScanner ${KYLIN_CLASSPATH_DEBUG}
    fi
}

function runTool() {
    retrieveDependency

    # get KYLIN_EXTRA_START_OPTS
    if [ -f "${dir}/setenv-tool.sh" ]; then
        source ${dir}/setenv-tool.sh
    fi
    
    verbose "java opts is ${KYLIN_EXTRA_START_OPTS} ${KYLIN_TOOL_OPTS}"
    verbose "java classpath is ${KYLIN_TOOL_CLASSPATH}"
    classpathDebug ${KYLIN_TOOL_CLASSPATH}

    exec $JAVA ${KYLIN_EXTRA_START_OPTS} ${KYLIN_TOOL_OPTS} -classpath ${KYLIN_TOOL_CLASSPATH}  "$@"
}

mkdir -p ${KYLIN_HOME}/logs
mkdir -p ${KYLIN_HOME}/ext

# start command
if [ "$1" == "start" ]
then
    if [ -f "${KYLIN_HOME}/pid" ]
    then
        PID=`cat $KYLIN_HOME/pid`
        if ps -p $PID > /dev/null
        then
          quit "Kylin is running, stop it first"
        fi
    fi

    checkBasicKylinProps

    #retrive $KYLIN_EXTRA_START_OPTS
    if [ -f "${dir}/setenv.sh" ]; then
        echo "WARNING: ${dir}/setenv.sh is deprecated and ignored, please remove it and use ${KYLIN_HOME}/conf/setenv.sh instead"
        source ${dir}/setenv.sh
    fi

    if [ -f "${KYLIN_HOME}/conf/setenv.sh" ]; then
        source ${KYLIN_HOME}/conf/setenv.sh
    fi


    ${dir}/check-env.sh "if-not-yet" || exit 1

    retrieveDependency

    verbose "java classpath is ${KYLIN_TOMCAT_CLASSPATH}"
    verbose "java opts is ${KYLIN_EXTRA_START_OPTS} ${KYLIN_TOMCAT_OPTS}"

    checkRestPort

    # kickoff spark-client
    columnarEnabled=`${dir}/get-properties.sh kap.storage.columnar.start-own-spark`
    if [ "${columnarEnabled}" == "true" ]
    then
        ${dir}/spark-client.sh start
    fi


    classpathDebug ${KYLIN_TOMCAT_CLASSPATH}

    $JAVA ${KYLIN_EXTRA_START_OPTS} ${KYLIN_TOMCAT_OPTS} -classpath ${KYLIN_TOMCAT_CLASSPATH}  org.apache.catalina.startup.Bootstrap start >> ${KYLIN_HOME}/logs/kylin.out 2>&1 & echo $! > ${KYLIN_HOME}/pid &

    echo ""
    echo "A new KAP server is started by $USER. To stop it, run 'kylin.sh stop'"
    echo "Check the log at ${KYLIN_HOME}/logs/kylin.log"
    echo "Web UI is at http://${KYLIN_REST_ADDRESS}/kylin"
    exit 0

# stop command
elif [ "$1" == "stop" ]
then

    columnarEnabled=`${dir}/get-properties.sh kap.storage.columnar.start-own-spark`
    if [ "${columnarEnabled}" == "true" ]
    then
        ${dir}/spark-client.sh stop
    fi

    if [ -f "${KYLIN_HOME}/pid" ]
    then
        PID=`cat $KYLIN_HOME/pid`
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
           exit 0
        else
           quit "Kylin is not running"
        fi

    else
        quit "Kylin is not running"
    fi

elif [ "$1" = "version" ]
then
    runTool org.apache.kylin.common.KylinVersion

elif [ "$1" = "diag" ]
then
    quit "kylin.sh diag no longer supported, use 'bin/diag.sh' instead"

elif [ "$1" = "admin-password-reset" ]
then
    runTool io.kyligence.kap.tool.security.KapPasswordResetCLI

# tool command
elif [[ "$1" = org.apache.kylin.* ]] || [[ "$1" = io.kyligence.* ]]
then
    runTool "$@"

else
    quit "Usage: 'kylin.sh [-v] start' or 'kylin.sh [-v] stop'"
fi
