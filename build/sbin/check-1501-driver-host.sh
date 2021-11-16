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
#title=Checking Spark Driver Host

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
source ${KYLIN_HOME}/sbin/init-kerberos.sh

## init Kerberos if needed
initKerberosIfNeeded

echo "Checking Spark Driver Host..."

# for query
kylin_storage_host=`$KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.spark-conf.spark.driver.host`

# for build
kylin_engine_master=`$KYLIN_HOME/bin/get-properties.sh kylin.engine.spark-conf.spark.master`
kylin_engine_host=`$KYLIN_HOME/bin/get-properties.sh kylin.engine.spark-conf.spark.driver.host`

#ipv4 or ipv6
ip_reg='^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^([0-9a-fA-F]{1,4}:){6}((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^::([0-9a-fA-F]{1,4}:){0,4}((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^([0-9a-fA-F]{1,4}:):([0-9a-fA-F]{1,4}:){0,3}((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^([0-9a-fA-F]{1,4}:){2}:([0-9a-fA-F]{1,4}:){0,2}((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^([0-9a-fA-F]{1,4}:){3}:([0-9a-fA-F]{1,4}:){0,1}((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^([0-9a-fA-F]{1,4}:){4}:((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$|^:((:[0-9a-fA-F]{1,4}){1,6}|:)$|^[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,5}|:)$|^([0-9a-fA-F]{1,4}:){2}((:[0-9a-fA-F]{1,4}){1,4}|:)$|^([0-9a-fA-F]{1,4}:){3}((:[0-9a-fA-F]{1,4}){1,3}|:)$|^([0-9a-fA-F]{1,4}:){4}((:[0-9a-fA-F]{1,4}){1,2}|:)$|^([0-9a-fA-F]{1,4}:){5}:([0-9a-fA-F]{1,4})?$|^([0-9a-fA-F]{1,4}:){6}:$'

# 0:succeed 1:ERROR 4:WARN
status=0
log_str="Current kylin_engine_master is '$kylin_engine_master'.\n"

# kylin_storage_host need to be checked every time
if [ -z $kylin_storage_host ];then
  log_str=$log_str"WARN: 'kylin.storage.columnar.spark-conf.spark.driver.host' is missed, it may cause some problems.\n"
  status=4
else
  if [[ "$kylin_storage_host" =~ $ip_reg ]];then
    log_str=$log_str"PASS: 'kylin.storage.columnar.spark-conf.spark.driver.host' is valid, checking passed.\n"
  else
    log_str=$log_str"ERROR: 'kylin.storage.columnar.spark-conf.spark.driver.host:' '$kylin_storage_host' is not a valid ip address, checking failed.\n"
    status=1
  fi
fi

# kylin_engine_host need to be a valid ip address when kylin_engine_master is "yarn-client" or "yarn"
# and it should not be set when kylin_engine_master is "yarn-cluster"
# check engine driver host
if [[ "$kylin_engine_master" =~ .*cluster$ ]];then
  if [ -z $kylin_engine_host ];then
    log_str=$log_str"PASS: 'kylin.engine.spark-conf.spark.driver.host' should not be set when kylin_engine_master is 'yarn-cluster', checking passed"
  else
    log_str=$log_str"ERROR: 'kylin.engine.spark-conf.spark.driver.host'should not be set when kylin_engine_master is 'yarn-cluster', but it is set as '$kylin_engine_host', checking failed"
    status=1
  fi
else
  if [[ "$kylin_engine_host" =~ $ip_reg ]];then
    log_str=$log_str"PASS: 'kylin.engine.spark-conf.spark.driver.host' is valid, checking passed."
  elif [ -z $kylin_engine_host ];then
    log_str=$log_str"WARN: 'kylin.engine.spark-conf.spark.driver.host' is missed, it may cause some problems."
    if [ $status = 0 ];then
      status=4
    fi
  else
    log_str=$log_str"ERROR: 'kylin.engine.spark-conf.spark.driver.host:' '$kylin_engine_host' is not a valid ip address, checking failed."
    status=1
  fi
fi

echo -e $log_str

if [ $status = 0 ];then
  echo "Checking Spark Driver Host succeed"
  exit 0
elif [ $status = 1 ];then
  quit "ERROR: $log_str"
elif [ $status = 4 ]; then
  exit 4
fi