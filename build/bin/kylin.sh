#!/bin/bash
# Kyligence Inc. License

# set verbose=true to print more logs in scripts
source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh $@

if [ "$1" == "-v" ]; then
    shift
fi

if [ "${ENABLE_CHECK_ENV}" != false ]; then
    source ${dir}/check-env.sh "if-not-yet"
fi

function retrieveDependency() {
    #retrive $hive_dependency and $hbase_dependency
    if [ -z "${hive_dependency}" ]; then
	    source ${dir}/find-hive-dependency.sh
    fi
    if [ -z "${hbase_dependency}" ]; then
	    source ${dir}/find-hbase-dependency.sh
    fi
    
    #retrive $KYLIN_EXTRA_START_OPTS
    if [ -f "${dir}/setenv.sh" ]; then
        source ${dir}/setenv.sh
    fi

    export HBASE_CLASSPATH_PREFIX=${KYLIN_HOME}/conf:${KYLIN_HOME}/lib/*:${KYLIN_HOME}/kybot/*:${KYLIN_HOME}/ext/*:${HBASE_CLASSPATH_PREFIX}
    export HBASE_CLASSPATH=${HBASE_CLASSPATH}:${hive_dependency}
    if [ -n "${KAFKA_HOME}" ]; then
        source ${dir}/find-kafka-dependency.sh
        export HBASE_CLASSPATH=${HBASE_CLASSPATH}:${kafka_dependency}
    fi
    
    source ${dir}/find-hadoop-conf-dir.sh
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

    
    kapVersion=`${dir}/get-properties.sh kap.version` 
    fileVersion=`cat ${KYLIN_HOME}/VERSION`
    
    if [ "${kapVersion}" != "${fileVersion}" ]
    then
        quit 'Did you copied and replaced conf/kylin.properties from somewhere else (when upgrading)? It is not supported. Please strictly follow upgrade manual to apply previous changes to shipped kylin.properties in this version'
    fi

    # check metadata store type
    metadataUrl=`${dir}/get-properties.sh kylin.metadata.url`
    if [[ "${metadataUrl##*@}" != "hbase" ]]
    then
        exec ${dir}/kylin-start-wo-hbase.sh
    fi
    
    retrieveDependency

    columnarEnabled=`${dir}/get-properties.sh kap.storage.columnar.start-own-spark`
    if [ "${columnarEnabled}" == "true" ]
    then
        echo "Calling spark client start..."
        ${dir}/spark_client.sh start
    fi
    
    tomcat_root=${dir}/../tomcat
    export tomcat_root

    #The location of all hadoop/hbase configurations are difficult to get.
    #Plus, some of the system properties are secretly set in hadoop/hbase shell command.
    #For example, in hdp 2.2, there is a system property called hdp.version,
    #which we cannot get until running hbase or hadoop shell command.
    #
    #To save all these troubles, we use hbase runjar to start tomcat.
    #In this way we no longer need to explicitly configure hadoop/hbase related classpath for tomcat,
    #hbase command will do all the dirty tasks for us:

    spring_profile=`${dir}/get-properties.sh kylin.security.profile`
    if [ -z "$spring_profile" ]
    then
        quit "please set kylin.security.profile in kylin.properties, options are: testing, ldap, saml."
    else
        verbose "kylin.security.profile is set to $spring_profile"
    fi

    #additionally add tomcat libs to HBASE_CLASSPATH_PREFIX
    export HBASE_CLASSPATH_PREFIX=${tomcat_root}/bin/bootstrap.jar:${tomcat_root}/bin/tomcat-juli.jar:${tomcat_root}/lib/*:${HBASE_CLASSPATH_PREFIX}

    if [ -z "$KYLIN_REST_ADDRESS" ]
    then
        kylin_rest_address=`hostname -f`":"`grep "<Connector port=" ${tomcat_root}/conf/server.xml |grep protocol=\"HTTP/1.1\" | cut -d '=' -f 2 | cut -d \" -f 2`
    else
        kylin_rest_address=$KYLIN_REST_ADDRESS
    fi
    verbose "kap.job.helix.host-address is set to ${kylin_rest_address}"

    kylin_rest_address_arr=(${kylin_rest_address//:/ })
    nc -z -w 5 ${kylin_rest_address_arr[0]} ${kylin_rest_address_arr[1]} 1>/dev/null 2>&1; nc_result=$?
    if [ $nc_result -eq 0 ]; then
        quit "Port ${kylin_rest_address} is not available, could not start Kylin"
    fi

    #debug if encounter NoClassDefError
    verbose "kylin classpath is: $(hbase classpath)"

    # KYLIN_EXTRA_START_OPTS is for customized settings, checkout bin/setenv.sh
    hbase ${KYLIN_EXTRA_START_OPTS} \
    -Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager \
    -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-server-log4j.properties \
    -Dorg.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH=true \
    -Dorg.apache.catalina.connector.CoyoteAdapter.ALLOW_BACKSLASH=true \
    -Djava.endorsed.dirs=${tomcat_root}/endorsed  \
    -Dcatalina.base=${tomcat_root} \
    -Dcatalina.home=${tomcat_root} \
    -Djava.io.tmpdir=${tomcat_root}/temp  \
    -Dkylin.hive.dependency=${hive_dependency} \
    -Dkylin.hbase.dependency=${hbase_dependency} \
    -Dkylin.kafka.dependency=${kafka_dependency} \
    -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} \
    -Dkap.job.helix.host-address=${kylin_rest_address} \
    -Dspring.profiles.active=${spring_profile} \
    org.apache.hadoop.util.RunJar ${tomcat_root}/bin/bootstrap.jar  org.apache.catalina.startup.Bootstrap start >> ${KYLIN_HOME}/logs/kylin.out 2>&1 & echo $! > ${KYLIN_HOME}/pid &

    echo ""
    echo "A new Kylin instance is started by $USER. To stop it, run 'kylin.sh stop'"
    echo "Check the log at ${KYLIN_HOME}/logs/kylin.log"
    config_file_path=${KYLIN_HOME}/conf/kylin.properties
    kylin_server_port=`sed -n "s/<Connector port=\"\(.*\)\" protocol=\"HTTP\/1.1\"/\1/"p ${KYLIN_HOME}/tomcat/conf/server.xml`
    kylin_server_port=`echo ${kylin_server_port}` #ignore white space
    echo "Web UI is at http://<hostname>:${kylin_server_port}/kylin"
    exit 0

# stop command
elif [ "$1" == "stop" ]
then

    columnarEnabled=`$KYLIN_HOME/bin/get-properties.sh kap.storage.columnar.start-own-spark`
    if [ "${columnarEnabled}" == "true" ]
    then
        ${dir}/spark_client.sh stop
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
    retrieveDependency
    exec hbase -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties org.apache.kylin.common.KylinVersion
    exit 0

elif [ "$1" = "diag" ]
then
    echo "kylin.sh diag no longer supported, use diag.sh instead"
    exit 0

# tool command
elif [[ "$1" = org.apache.kylin.* ]] || [[ "$1" = io.kyligence.* ]]
then
    retrieveDependency

    #retrive $KYLIN_EXTRA_START_OPTS from a separate file called setenv-tool.sh
    unset KYLIN_EXTRA_START_OPTS # unset the global server setenv config first
    if [ -f "${dir}/setenv-tool.sh" ]; then
        source ${dir}/setenv-tool.sh
    fi

    hbase_original=${HBASE_CLASSPATH}
    export HBASE_CLASSPATH=${hbase_original}:${KYLIN_HOME}/tool/*
    exec hbase ${KYLIN_EXTRA_START_OPTS} -Dkylin.hive.dependency=${hive_dependency} -Dkylin.hbase.dependency=${hbase_dependency} -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties "$@"
    export HBASE_CLASSPATH=${hbase_original}

else
    quit "Usage: 'kylin.sh [-v] start' or 'kylin.sh [-v] stop'"
fi
