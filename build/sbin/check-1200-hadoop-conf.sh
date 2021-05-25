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

#title=Checking Hadoop Configuration

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
source ${KYLIN_HOME}/sbin/init-kerberos.sh
source ${KYLIN_HOME}/sbin/prepare-hadoop-env.sh

## init Kerberos if needed
initKerberosIfNeeded

source ${KYLIN_HOME}/sbin/prepare-hadoop-conf-dir.sh

echo "Checking hadoop conf dir..."

[[ -z "${kylin_hadoop_conf_dir}" ]] && quit "ERROR: Failed to find Hadoop config dir, please set kylin_hadoop_conf_dir."

prepare_hadoop_conf_jars

# this is the very first check, apply -v to print verbose classpath in check-env log
${KYLIN_HOME}/sbin/bootstrap.sh -v io.kyligence.kap.tool.hadoop.CheckHadoopConfDir "${kylin_hadoop_conf_dir}"

# CheckHadoopConfDir will print the last error message
[[ $? == 0 ]] || quit "ERROR: Check HADOOP_CONF_DIR failed. Please correct hadoop configurations."

function getSourceFile() {

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

    if [[ -n $FI_ENV_PLATFORM ]]; then
        if [[ $1 == "hive-site.xml" ]]; then
            echo "${FI_ENV_PLATFORM}/Hive/config/hive-site.xml"
            return
        fi
        echo "${FI_ENV_PLATFORM}/HDFS/hadoop/etc/hadoop/$1"
        return
    fi

    if [[ -d "/etc/hadoop/conf" ]]; then
        if [[ $1 == "hive-site.xml" ]]; then
            echo "/etc/hive/conf/hive-site.xml"
            return
        fi
        echo "/etc/hadoop/conf/$1"
        return
    fi
}

if [[ ! -f ${kylin_hadoop_conf_dir}/core-site.xml ]]; then
    source_file=$(getSourceFile "core-site.xml")
    [[ -z ${source_file} ]] || quit "core-site.xml does not exist in ${kylin_hadoop_conf_dir}, please copy it from ${source_file}"
fi

if [[ ! -f ${kylin_hadoop_conf_dir}/hdfs-site.xml ]]; then
    source_file=$(getSourceFile "hdfs-site.xml")
    [[ -z ${source_file} ]] || quit "hdfs-site.xml does not exist in ${kylin_hadoop_conf_dir}, please copy it from ${source_file}"
fi

if [[ ! -f ${kylin_hadoop_conf_dir}/yarn-site.xml ]]; then
    source_file=$(getSourceFile "yarn-site.xml")
    [[ -z ${source_file} ]] || quit "yarn-site.xml does not exist in ${kylin_hadoop_conf_dir}, please copy it from ${source_file}"
fi

if [[ ! -f ${kylin_hadoop_conf_dir}/hive-site.xml ]]; then
    source_file=$(getSourceFile "hive-site.xml")
    [[ -z ${source_file} ]] || quit "hive-site.xml does not exist in ${kylin_hadoop_conf_dir}, please copy it from ${source_file}"
fi

if [[ $(is_kap_kerberos_enabled) == 1 && ! -f ${kylin_hadoop_conf_dir}/$KYLIN_KRB5CONF ]]; then
    quit "krb5.conf does not exist in ${kylin_hadoop_conf_dir}, please copy it from ${KYLIN_HOME}/conf/${KYLIN_KRB5CONF}"
fi