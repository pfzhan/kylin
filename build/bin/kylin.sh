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
#    export KYLIN_TOMCAT_CLASSPATH=`$JAVA -cp ${KYLIN_HOME}/tool/kylin-tool-kap-*.jar io.kyligence.kap.tool.mr.ClassPathFilter ${KYLIN_TOMCAT_CLASSPATH} jersey  spark-unsafe`

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
    #export env
    export CONF_DIR=${KYLIN_HOME}/conf
    export LOG4J_DIR=${KYLIN_HOME}/conf
    export SPARK_DIR=${KYLIN_HOME}/spark/
    export KYLIN_SPARK_TEST_JAR_PATH=`ls $KYLIN_HOME/tool/kylin-tool-kap-*.jar`
    export KYLIN_SPARK_JAR_PATH=`ls $KYLIN_HOME/lib/kylin-storage-parquet-kap-*.jar`
    export KAP_HDFS_WORKING_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.env.hdfs-working-dir`
    export KAP_METADATA_URL=`$KYLIN_HOME/bin/get-properties.sh kylin.metadata.url`
    if [ -z "$ZIPKIN_HOSTNAME" ]
    then
        export ZIPKIN_HOSTNAME=`hostname`
    fi
    if [ -z "$ZIPKIN_SCRIBE_PORT" ]
    then
        export ZIPKIN_SCRIBE_PORT="9410"
    fi
    echo "ZIPKIN_HOSTNAME is set to ${ZIPKIN_HOSTNAME}"
    echo "ZIPKIN_SCRIBE_PORT is set to ${ZIPKIN_SCRIBE_PORT}"
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

    #retrive $KYLIN_EXTRA_START_OPTS
    if [ -f "${dir}/setenv.sh" ]; then
        echo "WARNING: ${dir}/setenv.sh is deprecated and ignored, please remove it and use ${KYLIN_HOME}/conf/setenv.sh instead"
        source ${dir}/setenv.sh
    fi
    
    if [ -f "${KYLIN_HOME}/conf/setenv.sh" ]; then
        source ${KYLIN_HOME}/conf/setenv.sh
    fi
    export SPARK_DIR=${KYLIN_HOME}/spark/
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
    if [[ -z $HIVE_METASTORE_URI ]]
    then
     source ${dir}/find-hive-dependency.sh
    HIVE_METASTORE_URI=$(${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.mr.HadoopConfPropertyRetriever ${hive_conf_path}/hive-site.xml hive.metastore.uris | grep -v Retrieving | tail -1)
    fi
    if [[ -z "$HIVE_METASTORE_URI" ]]
    then
        quit "Couldn't find hive.metastore.uris in hive-site.xml. hive.metastore.uris specifies Thrift URI of hive metastore . Please export HIVE_METASTORE_URI with hive.metastore.uris before starting kylin , for example: export HIVE_METASTORE_URI=thrift://sandbox.hortonworks.com:9083"
    fi
    classpathDebug ${KYLIN_TOMCAT_CLASSPATH}
    export KYLIN_TOMCAT_CLASSPATH=`$JAVA -cp ${KYLIN_HOME}/tool/kylin-tool-kap-*.jar io.kyligence.kap.tool.mr.ClassPathFilter ${KYLIN_TOMCAT_CLASSPATH} jersey  spark-unsafe`
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
#    if [ "${columnarEnabled}" == "true" ]
#    then
#        ${dir}/spark-client.sh stop
#    fi

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
