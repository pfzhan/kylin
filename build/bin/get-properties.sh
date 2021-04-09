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

if [ $# != 1 ]
then
    if [[ $# < 2 || $2 != 'DEC' ]]
        then
            echo 'invalid input'
            exit 1
    fi
fi

if [ -z $KYLIN_HOME ];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

if [[ -z ${MAPR_HOME} ]];then
    export MAPR_HOME="/opt/mapr"
fi

if [ -z ${kylin_hadoop_conf_dir} ]; then
    export kylin_hadoop_conf_dir=$KYLIN_HOME/hadoop_conf
fi

export KYLIN_KERBEROS_OPTS=""
if [ -f ${KYLIN_HOME}/conf/krb5.conf ];then
    KYLIN_KERBEROS_OPTS="-Djava.security.krb5.conf=${KYLIN_HOME}/conf/krb5.conf"
fi

export SPARK_HOME=$KYLIN_HOME/spark

if [[ -f ${KYLIN_HOME}/conf/kylin-tools-log4j.xml ]]; then
    kylin_tools_log4j="file:${KYLIN_HOME}/conf/kylin-tools-log4j.xml"
    else
    kylin_tools_log4j="file:${KYLIN_HOME}/tool/conf/kylin-tools-log4j.xml"
fi

if [[ $(hadoop version) == *"mapr"* ]]; then
    MAPR_AUTHENTICATION="-Djava.security.auth.login.config=${MAPR_HOME}/conf/mapr.login.conf"
fi

mkdir -p ${KYLIN_HOME}/logs
result=`java ${KYLIN_KERBEROS_OPTS} -Dlog4j.configurationFile=${kylin_tools_log4j} -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} -Dhdp.version=current ${MAPR_AUTHENTICATION} -cp "${kylin_hadoop_conf_dir}:${KYLIN_HOME}/lib/ext/*:${KYLIN_HOME}/server/jars/*:${SPARK_HOME}/jars/*" io.kyligence.kap.tool.KylinConfigCLI $@ 2>>${KYLIN_HOME}/logs/shell.stderr`

echo "$result"