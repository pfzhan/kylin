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

if [[ -z ${cdh_mapreduce_path} ]]
then
    if [[ -d "/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce" ]]
    then
        cdh_mapreduce_path="/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce"
    else
        cdh_mapreduce_path="/usr/lib/hadoop-mapreduce"
    fi
fi

function isCDH_6_1() {
    hadoop_common_file=`find ${cdh_mapreduce_path}/../../jars/ -maxdepth 1 -name "hadoop-common-*.jar" -not -name "*test*" | tail -1`
    cdh_version=${hadoop_common_file##*/}

    if [[ "${cdh_version}" == hadoop-common-3.*-cdh6.1* ]]; then
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

function isHDP_3_1() {
    hadoop_common_file="`find ${hdp_hadoop_path}/ -maxdepth 1 -name hadoop-common-*.jar -not -name *test* | tail -1`"
    hdp_version=${hadoop_common_file##*/}

    if [[ "${hdp_version}" == hadoop-common-3.1* ]]; then
        echo 1
        return 1
    fi

    echo 0
    return 0
}

function isHDP_2_6() {
    hadoop_common_file="`find ${hdp_hadoop_path}/ -maxdepth 1 -name hadoop-common-*.jar -not -name *test* | tail -1`"
    hdp_version=${hadoop_common_file##*/}

    if [[ "${hdp_version}" == hadoop-common-2*2.6.* ]]; then
        echo 1
        return 1
    fi

    echo 0
    return 0
}

function isFI_C90() {
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