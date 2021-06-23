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

#title=Checking Permission of HDFS Working Dir

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source ${KYLIN_HOME}/sbin/find-working-dir.sh
source ${KYLIN_HOME}/sbin/init-kerberos.sh

## init Kerberos if needed
initKerberosIfNeeded

echo "Checking HDFS working dir..."

RANDNAME=chkenv__${RANDOM}
TEST_FILE=${WORKING_DIR}/${RANDNAME}

# test local hdfs
## in read-write separation mode this is write cluster
hadoop ${hadoop_conf_param} fs -test -d ${WORKING_DIR} || quit "ERROR: Please check the correctness of hadoop configuration file under ${kylin_hadoop_conf_dir}. At the same time, please make sure the working directory '${WORKING_DIR}' exists and grant access permission to current user."

# test if kylin user (current user) has write permission to working directory
touch ./${RANDNAME}
hadoop ${hadoop_conf_param} fs -put -f ./${RANDNAME} ${TEST_FILE} || quit "ERROR: Have no permission to create/modify file in working directory '${WORKING_DIR}'. Please grant permission to current user."

rm -f ./${RANDNAME}
if [ -n "$TDH_CLIENT_HOME" ]
then
  hadoop ${hadoop_conf_param} fs -rm ${TEST_FILE}
else
  hadoop ${hadoop_conf_param} fs -rm -skipTrash ${TEST_FILE}
fi
