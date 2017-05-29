#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh $@

#
# FIXME: NEEDS REFACTOR with kylin.sh
#

function retrieveDependency() {

    if [ -z "${hive_dependency}" ]; then
	    source ${dir}/find-hive-dependency.sh
    fi


    if [ -z "${hdp_version}" ]; then
        hdp_version=`/bin/bash -x hadoop 2>&1 | sed -n "s/\(.*\)export HDP_VERSION=\(.*\)/\2/"p`
    fi

    #retrive $KYLIN_EXTRA_START_OPTS
    if [ -f "${dir}/setenv.sh" ]; then
        source ${dir}/setenv.sh
    fi
    export KYLIN_TOMCAT_CLASSPATH=`hadoop classpath`
    export KYLIN_TOMCAT_CLASSPATH_PREFIX=${KYLIN_HOME}/conf:${KYLIN_HOME}/lib/*:${KYLIN_HOME}/kybot/*:${KYLIN_HOME}/ext/*:${KYLIN_TOMCAT_CLASSPATH_PREFIX}
    export KYLIN_TOMCAT_CLASSPATH=${KYLIN_TOMCAT_CLASSPATH}:${hive_dependency}
    if [ -n "${KAFKA_HOME}" ]; then
        source ${dir}/find-kafka-dependency.sh
        export KYLIN_TOMCAT_CLASSPATH=${KYLIN_TOMCAT_CLASSPATH}:${kafka_dependency}
    fi

    source ${dir}/find-hadoop-conf-dir.sh
}

    retrieveDependency

    columnarEnabled=`${dir}/get-properties.sh kap.storage.columnar.start-own-spark`
    if [ "${columnarEnabled}" == "true" ]
    then
        echo "Calling spark client start..."
        ${dir}/spark_client.sh start
    fi

    tomcat_root=${dir}/../tomcat
    export tomcat_root



    spring_profile=`${dir}/get-properties.sh kylin.security.profile`
    if [ -z "$spring_profile" ]
    then
        quit 'please set kylin.security.profile in kylin.properties, options are: testing, ldap, saml.'
    else
        verbose "kylin.security.profile is set to $spring_profile"
    fi


    export KYLIN_TOMCAT_CLASSPATH_PREFIX=${tomcat_root}/bin/bootstrap.jar:${tomcat_root}/bin/tomcat-juli.jar:${tomcat_root}/lib/*:${KYLIN_TOMCAT_CLASSPATH_PREFIX}

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
    export KYLIN_TOMCAT_CLASSPATH=${KYLIN_TOMCAT_CLASSPATH_PREFIX}:${KYLIN_TOMCAT_CLASSPATH}
    # KYLIN_EXTRA_START_OPTS is for customized settings, checkout bin/setenv.sh

    export KYLIN_TOMCAT_OPTS="${KYLIN_EXTRA_START_OPTS}   -Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager \
    -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-server-log4j.properties \
    -Dorg.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH=true \
    -Dorg.apache.catalina.connector.CoyoteAdapter.ALLOW_BACKSLASH=true \
    -Djava.endorsed.dirs=${tomcat_root}/endorsed  \
    -Dcatalina.base=${tomcat_root} \
    -Dcatalina.home=${tomcat_root} \
    -Djava.io.tmpdir=${tomcat_root}/temp  \
    -Dkylin.hive.dependency=${hive_dependency} \
    -Dkylin.kafka.dependency=${kafka_dependency} \
    -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} \
    -Dkap.job.helix.host-address=${kylin_rest_address} \
    -Dspring.profiles.active=${spring_profile} \
    -Dhdp.version=${hdp_version}"

    export KYLIN_TOMCAT_CLASSPATH=${tomcat_root}/bin/bootstrap.jar:${KYLIN_TOMCAT_CLASSPATH}
    java -classpath ${KYLIN_TOMCAT_CLASSPATH} ${KYLIN_TOMCAT_OPTS}  org.apache.catalina.startup.Bootstrap start >> ${KYLIN_HOME}/logs/kylin.out 2>&1 & echo $! > ${KYLIN_HOME}/pid &

    echo ""
    echo "A new Kylin instance is started by $USER. To stop it, run 'kylin.sh stop'"
    echo "Check the log at ${KYLIN_HOME}/logs/kylin.log"
    config_file_path=${KYLIN_HOME}/conf/kylin.properties
    kylin_server_port=`sed -n "s/<Connector port=\"\(.*\)\" protocol=\"HTTP\/1.1\"/\1/"p ${KYLIN_HOME}/tomcat/conf/server.xml`
    kylin_server_port=`echo ${kylin_server_port}` #ignore white space
    echo "Web UI is at http://<hostname>:${kylin_server_port}/kylin"
    exit 0