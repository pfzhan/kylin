#!/bin/bash
# Kyligence Inc. License

# source me

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

function fetchKylinHadoopConf() {

    export FI_ENV_PLATFORM=

    ## FusionInsight platform C60.
    if [ -n "$BIGDATA_HOME" ]
    then
        FI_ENV_PLATFORM=$BIGDATA_HOME
    fi

    ## FusionInsight platform C70.
    if [ -n "$BIGDATA_CLIENT_HOME" ]
    then
        FI_ENV_PLATFORM=$BIGDATA_CLIENT_HOME
    fi

    if [[ -d ${kylin_hadoop_conf_dir} ]]; then
        return
    fi

    if [ -n "$FI_ENV_PLATFORM" ]
    then
        mkdir -p ${KYLIN_HOME}/hadoop_conf
        # FI platform
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/core-site.xml
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/hdfs-site.xml
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/yarn-site.xml
        checkAndCopyFile $FI_ENV_PLATFORM/Hive/config/hive-site.xml
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/mapred-site.xml

        # Spark need hive-site.xml in FI
        checkAndCopyFile $FI_ENV_PLATFORM/Hive/config/hive-site.xml ${SPARK_HOME}/conf

        # don't find topology.map in FI
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/topology.py
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/ssl-client.xml
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/hadoop-env.sh

    elif [ -d "/etc/hadoop/conf" ]
    then
        mkdir -p ${KYLIN_HOME}/hadoop_conf
        # CDH/HDP platform

        checkAndCopyFile /etc/hadoop/conf/core-site.xml
        checkAndCopyFile /etc/hadoop/conf/hdfs-site.xml
        checkAndCopyFile /etc/hadoop/conf/yarn-site.xml
        checkAndCopyFile /etc/hive/conf/hive-site.xml
        checkAndCopyFile /etc/hadoop/conf/mapred-site.xml

        checkAndCopyFile /etc/hadoop/conf/topology.py
        checkAndCopyFile /etc/hadoop/conf/topology.map
        checkAndCopyFile /etc/hadoop/conf/ssl-client.xml
        checkAndCopyFile /etc/hadoop/conf/hadoop-env.sh
    else
        if [ -f "${kylin_hadoop_conf_dir}/hdfs-site.xml" ]
        then
            echo "Hadoop conf directory currently generated based on manual mode."
        else
            echo "Missing hadoop conf files. Please contact Kyligence technical support for more details."
            exit -1
        fi
    fi

    if [ -d ${KYLIN_HOME}/hadoop_conf_override ]
    then
        cp -rf ${KYLIN_HOME}/hadoop_conf_override/hive-site.xml ${kylin_hadoop_conf_dir}
    fi
}

function checkAndCopyFile() {
    source_file=$1
    if [[ -f ${source_file} ]]; then
        if [[ -n $2 ]]; then
            dst_dir=$2
        else
            dst_dir=${kylin_hadoop_conf_dir}
        fi
        cp -rf ${source_file} ${dst_dir}
    fi
}

if [[ "$kylin_hadoop_conf_dir" == "" ]]
then
    verbose Retrieving hadoop config dir...

    export kylin_hadoop_conf_dir=${KYLIN_HOME}/hadoop_conf

    fetchKylinHadoopConf
fi

