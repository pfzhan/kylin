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

BYPASS=${KYLIN_HOME}/server/jars/replace-jars-bypass
# only replace when has Kerberos
kerberosEnabled=`${KYLIN_HOME}/bin/get-properties.sh kylin.kerberos.enabled`
source ${KYLIN_HOME}/sbin/prepare-mrs-env.sh
if [[ "${kerberosEnabled}" == "false" || -f ${BYPASS} ]]
then
    return
fi

echo "Start replacing zookeeper jars under ${KYLIN_HOME}/server/jars."

zookeeper_jars=
if [ -n "$IS_MRS_PLATFORM" ]
then
   zookeeper_jars=$(find $FI_ENV_PLATFORM/HDFS/hadoop/share/hadoop/common -maxdepth 2 \
   -name "zookeeper-3.5.6-hw-ei-302*.jar" -not -name "*test*" \
   -o -name "zookeeper-jute-3.5.6-hw-ei-302*.jar" -not -name "*test*")
   echo "Find platform specific jars:${zookeeper_jars}, will replace with these jars under ${KYLIN_HOME}/server/jars."

   for jar_file in ${zookeeper_jars}
   do
      `cp ${jar_file} ${KYLIN_HOME}/server/jars`
   done
   touch ${BYPASS}
fi

echo "Done zookeeper jars replacement under ${KYLIN_HOME}/server/jars."
