#!/bin/bash

##
## Copyright (C) 2020 Kyligence Inc. All rights reserved.
##
## http://kyligence.io
##
## This software is the confidential and proprietary information of
## Kyligence Inc. ("Confidential Information"). You shall not disclose
## such Confidential Information and shall use it only in accordance
## with the terms of the license agreement you entered into with
## Kyligence Inc.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
## "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
## LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
## A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
## OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
## SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
## LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
## DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
## THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
## (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
## OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
##

# source me

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh

KYLIN_ENV_CHANNEL=`$KYLIN_HOME/bin/get-properties.sh kylin.env.channel`

KYLIN_KRB5CONF=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.krb5-conf`

function fetchCloudHadoopConf() {
    mkdir -p ${KYLIN_HOME}/hadoop_conf
    CLOUD_HADOOP_CONF_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.cloud.hadoop-conf-dir`
    checkAndCopyFile $CLOUD_HADOOP_CONF_DIR/core-site.xml
    checkAndCopyFile $CLOUD_HADOOP_CONF_DIR/hive-site.xml
    checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hiveserver2-site.xml
    checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hivemetastore-site.xml
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
        checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hiveserver2-site.xml
        checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hivemetastore-site.xml

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
        checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hiveserver2-site.xml
        checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hivemetastore-site.xml

        checkAndCopyFile /etc/hadoop/conf/topology.py
        checkAndCopyFile /etc/hadoop/conf/topology.map
        checkAndCopyFile /etc/hadoop/conf/ssl-client.xml
        checkAndCopyFile /etc/hadoop/conf/hadoop-env.sh
        # Ensure krb5.conf underlying hadoop_conf
        checkAndCopyFile $KYLIN_HOME/conf/$KYLIN_KRB5CONF
    else
        # APACHE HADOOP platform
        APACHE_HADOOP_CONF_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.env.apache-hadoop-conf-dir`
        if [ -n "${APACHE_HADOOP_CONF_DIR}" ]; then
            APACHE_HIVE_CONF_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.env.apache-hive-conf-dir`
            if [ -z "${APACHE_HIVE_CONF_DIR}" ]; then
                quit "ERROR: Please set kylin.env.apache-hive-conf-dir in kylin.properties."
            fi

            mkdir -p ${KYLIN_HOME}/hadoop_conf

            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/core-site.xml
            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/hdfs-site.xml
            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/yarn-site.xml
            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/mapred-site.xml

            checkAndCopyFile $APACHE_HIVE_CONF_DIR/hive-site.xml

            sed -i -r "/hive.metastore.schema.verification/I{n; s/true/false/}" ${KYLIN_HOME}/hadoop_conf/hive-site.xml

            checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hiveserver2-site.xml
            checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hivemetastore-site.xml

            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/topology.py
            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/topology.map
            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/ssl-client.xml
            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/hadoop-env.sh

            mysql_connector_jar="${KYLIN_HOME}/lib/ext/mysql-connector-*.jar"
            if [ ! -f "${mysql_connector_jar}" ]; then
              rm -rf ${KYLIN_HOME}/hadoop_conf
              echo "The mysql connector jar is missing, please place it in the ${KYLIN_HOME}/lib/ext directory."
              exit 1
            fi
            cp -rf "${mysql_connector_jar}" "${KYLIN_HOME}"/spark/jars/
        else
          if [ -f "${kylin_hadoop_conf_dir}/hdfs-site.xml" ]
          then
              echo "Hadoop conf directory currently generated based on manual mode."
          else
              echo "Missing hadoop conf files. Please contact Kyligence technical support for more details."
              exit -1
          fi
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

