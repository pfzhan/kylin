#!/bin/bash
# Kyligence Inc. License

# source me

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh

KYLIN_ENV_CHANNEL=`$KYLIN_HOME/bin/get-properties.sh kylin.env.channel`

function fetchCloudHadoopConf() {
    mkdir -p ${KYLIN_HOME}/hadoop_conf
    CLOUD_HADOOP_CONF_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.cloud.hadoop-conf-dir`
    checkAndCopyFile $CLOUD_HADOOP_CONF_DIR/core-site.xml
    checkAndCopyFile $CLOUD_HADOOP_CONF_DIR/hive-site.xml
}

function fetchKylinHadoopConf() {

    export FI_ENV_PLATFORM=

    ## FusionInsight platform C60.
    if [ -n "$BIGDATA_HOME" ]
    then
        FI_ENV_PLATFORM=$BIGDATA_HOME
    fi

    ## FusionInsight platform C70/C90.
    if [ -n "$BIGDATA_CLIENT_HOME" ]
    then
        FI_ENV_PLATFORM=$BIGDATA_CLIENT_HOME
    fi

    if [[ -d ${kylin_hadoop_conf_dir} ]]; then
        if [ -n "$FI_ENV_PLATFORM" ]; then
            checkAndCopyFIHiveSite
        fi
        return
    fi

    if [ -n "$FI_ENV_PLATFORM" ]
    then
        mkdir -p ${KYLIN_HOME}/hadoop_conf
        # FI platform
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/core-site.xml
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/hdfs-site.xml
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/yarn-site.xml
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/mapred-site.xml
        checkAndCopyFIHiveSite

        # don't find topology.map in FI
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/topology.py
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/ssl-client.xml
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/hadoop-env.sh

        if [ -n "$KRB5_CONFIG" ]
        then
          checkAndCopyFile $KRB5_CONFIG
        fi
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

# KE-9142 FI hive-site.xml is lack of some configuration
function checkAndCopyFIHiveSite() {

    checkAndCopyFile $FI_ENV_PLATFORM/Hive/config/hive-site.xml
    checkAndCopyFile $FI_ENV_PLATFORM/Hive/config/hivemetastore-site.xml
    hivesite_file=${kylin_hadoop_conf_dir}/hive-site.xml
    hivemeta_file=${kylin_hadoop_conf_dir}/hivemetastore-site.xml

    command -v xmllint > /dev/null || echo "ERROR: Command 'xmllint' is not accessible. Please install xmllint."
    if [[ -f ${hivemeta_file} ]] && [[ -f ${hivesite_file} ]]; then
        formartXML $hivemeta_file
        formartXML $hivesite_file
        metastore=$(echo "cat //configuration" |xmllint --shell $hivemeta_file| sed '/^\/ >/d'| sed '/configuration>/d')
        clean_content=$(echo $metastore | sed 's/\//\\\//g')
        if [[ -n $clean_content ]]; then
            sed -i "/<\/configuration.*>/ s/.*/${clean_content}&/" $hivesite_file
            formartXML $hivesite_file
        fi
    fi

    # Spark need hive-site.xml in FI
    checkAndCopyFile $hivesite_file ${SPARK_HOME}/conf
}

function formartXML() {
    file=$1
    if [[ -f ${file} ]]; then
        xmllint --format "$file" > "$file.xmlbak"
        if [[ $? == 0 ]]; then
            mv "$file.xmlbak" "$file"
        else
            echo "ERROR:  $file format error.Please check it."
        fi
    fi
}

if [[ "$kylin_hadoop_conf_dir" == "" ]]
then
    verbose Retrieving hadoop config dir...

    export kylin_hadoop_conf_dir=${KYLIN_HOME}/hadoop_conf

    export HADOOP_CONF_DIR=${KYLIN_HOME}/hadoop_conf

    if [[ ${KYLIN_ENV_CHANNEL} == "on-premises" || -z ${KYLIN_ENV_CHANNEL} ]]; then
        fetchKylinHadoopConf
    else
        fetchCloudHadoopConf
    fi
fi

