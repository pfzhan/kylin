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

cdh_mapreduce_path=$CDH_MR2_HOME
cdh_mapreduce_path_first="/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce"
cdh_mapreduce_path_second="/usr/lib/hadoop-mapreduce"
cdh_hadoop_lib_path="/opt/cloudera/parcels/CDH/lib/hadoop"
cdh_version=`hadoop version | head -1 | awk -F '-' '{print $2}'`

function quit {
    echo "$@"
    if [[ -n "${QUIT_MESSAGE_LOG}" ]]; then
        echo `setColor 31 "$@"` >> ${QUIT_MESSAGE_LOG}
    fi
    exit 1
}

function get_cdh_version_from_common_jar() {
    hadoop_common_path=`find ${cdh_mapreduce_path}/../hadoop/ -maxdepth 1 -name "hadoop-common-*.jar" -not -name "*test*" | tail -1`
    hadoop_common_jar=${hadoop_common_path##*-}
    echo "${hadoop_common_jar%.*}"
}

function set_cdh_mapreduce_path_with_hadoop_version_command() {
    cdh_mapreduce_path=${cdh_mapreduce_path_first}
    cdh_version_from_jar=`get_cdh_version_from_common_jar`

     # check cdh version from hadoop version command equals cdh version from common jar
    if [[ "${cdh_version}" == "${cdh_version_from_jar}" ]]; then
        return
    fi

    cdh_mapreduce_path=${cdh_mapreduce_path_second}
    cdh_version_from_jar=`get_cdh_version_from_common_jar`
    if [[ "${cdh_version}" != "${cdh_version_from_jar}" ]]; then
        quit "Get cdh mapreduce path failed, please set CDH_MR2_HOME environment variable."
    fi
}

if [[ -z ${cdh_mapreduce_path} ]]
then
    if [[ "${cdh_version}" == cdh* ]]; then
        set_cdh_mapreduce_path_with_hadoop_version_command
    else
        if [[ -d "${cdh_mapreduce_path_first}" ]]; then
            cdh_mapreduce_path=${cdh_mapreduce_path_first}
        else
            cdh_mapreduce_path=${cdh_mapreduce_path_second}
        fi
    fi
fi

function is_cdh_6_x() {
    hadoop_common_file=`find ${cdh_mapreduce_path}/../hadoop/ -maxdepth 1 -name "hadoop-common-*.jar" -not -name "*test*" | tail -1`
    cdh_version=${hadoop_common_file##*-}

    if [[ "${cdh_version}" == cdh6.* ]]; then
        echo 1
        return 1
    fi

    echo 0
    return 0
}

hdp_hadoop_path=$HDP_HADOOP_HOME
if [[ -z ${hdp_hadoop_path} ]]
then
    if [[ -d "/usr/hdp/current/hadoop-client" ]]; then
        hdp_hadoop_path="/usr/hdp/current/hadoop-client"
    else
        hdp_hadoop_path="/usr/hdp/*/hadoop"
    fi
fi

function is_hdp_3_x() {
    hadoop_common_file=$(find ${hdp_hadoop_path}/ -maxdepth 1 -name hadoop-common-*.jar -not -name "*test*" | tail -1)
    hdp_version=${hadoop_common_file##*/}

    if [[ "${hdp_version}" == hadoop-common-3.* ]]; then
        echo 1
        return 1
    fi

    echo 0
    return 0
}

function is_cdh_7_x() {
    hdp_version=`hadoop version | grep "CDH-7.*" | awk -F"/" '{print $5}'`

    if [[ -n "${hdp_version}" ]]; then
        echo 1
        return 1
    fi

    echo 0
    return 0
}

function is_hdp_2_6() {
    hadoop_common_file="`find ${hdp_hadoop_path}/ -maxdepth 1 -name hadoop-common-*.jar -not -name *test* | tail -1`"
    hdp_version=${hadoop_common_file##*/}

    if [[ "${hdp_version}" == hadoop-common-2*2.6.* ]]; then
        echo 1
        return 1
    fi

    echo 0
    return 0
}

function is_fi_c90() {
    ## FusionInsight platform C70/C90.
    if [[ -n "$BIGDATA_CLIENT_HOME" ]]; then
        hadoop_common_file="`find ${BIGDATA_CLIENT_HOME}/HDFS/hadoop/share/hadoop/common/ -maxdepth 1 -name hadoop-common-*.jar -not -name *test* | tail -1`"
        fi_version=${hadoop_common_file##*/}

        if [[ "${fi_version}" == hadoop-common-3.1* ]]; then
            echo 1
            return 1
        fi
    fi

    echo 0
    return 0
}

function prepare_hadoop_conf_jars(){
  if [[ $(is_cdh_7_x) == 1 ]]; then
    find ${SPARK_HOME}/jars -name "guava-*.jar" -exec rm -rf {} \;
    find ${KYLIN_HOME}/server/jars -name "guava-*.jar" -exec rm -rf {} \;

    guava_jars=$(find ${cdh_hadoop_lib_path}/client/ -maxdepth 1 \
    -name "guava-*.jar" \
    -o -name "failureaccess.jar")

    cp ${guava_jars} "${SPARK_HOME}"/jars
    cp ${guava_jars} "${KYLIN_HOME}"/server/jars
    hadoop_conf_cdh7x_jars=$(find ${cdh_hadoop_lib_path}/client/ -maxdepth 1 -name "commons-configuration2-*.jar")
    cp ${hadoop_conf_cdh7x_jars} ${SPARK_HOME}/jars
  fi
}