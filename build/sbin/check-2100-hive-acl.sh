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

#title=Checking Hive Acl

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source ${KYLIN_HOME}/sbin/init-kerberos.sh
## init Kerberos if needed
initKerberosIfNeeded

echo "Checking Hive Warehouse Dir Permission..."

if [ -z $KYLIN_HOME ];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

if [ -z ${kylin_hadoop_conf_dir} ]; then
    export kylin_hadoop_conf_dir=$KYLIN_HOME/hadoop_conf
fi

export KYLIN_KERBEROS_OPTS=""
if [ -f ${KYLIN_HOME}/conf/krb5.conf ];then
    KYLIN_KERBEROS_OPTS="-Djava.security.krb5.conf=${KYLIN_HOME}/conf/krb5.conf"
fi

export SPARK_HOME=$KYLIN_HOME/spark

tool_jar=$(ls $KYLIN_HOME/tool/kap-tool-*.jar)

java ${KYLIN_KERBEROS_OPTS} -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} -Dhdp.version=current -cp "${kylin_hadoop_conf_dir}:${KYLIN_HOME}/lib/ext/*:$tool_jar:${SPARK_HOME}/jars/*" io.kyligence.kap.tool.HiveAclCheckCLI $@ 2>>${KYLIN_HOME}/logs/shell.stderr

if [[ $? == 1 ]]; then
  quit "Checking Hive Acl false, please check curr user permission on dir hive.metastore.warehouse.dir."
fi