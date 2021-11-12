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

# flag variable to indicate whether the host is valid or not
storage_host_vaild=false
engine_host_vaild=false

# kylin_storage_host need to be checked every time
if [[ "$kylin_storage_host" =~ $ip_reg ]];then
  storage_host_vaild=true
fi

# kylin_engine_host need to be a valid ip address when kylin_engine_master is "yarn-client" or "yarn"
# and it should not be set when kylin_engine_master is "yarn-cluster"
if [[ "cluster" =~ $kylin_engine_master ]];then
  if [ ${#kylin_engine_host} = 0 ];then
    engine_host_vaild=true
  fi
elif [[ "$kylin_engine_host" =~ $ip_reg ]];then
    engine_host_vaild=true
fi

echo -e "Current kylin_engine_master is '$kylin_engine_master'\nNote: kylin_engine_host need to be a valid ip address when kylin_engine_master is 'yarn-client' or 'yarn'and it should not be set when kylin_engine_master is 'yarn-cluster'"
if $storage_host_vaild && $engine_host_vaild;then
  echo "kylin.storage.host '$kylin_storage_host' and kylin.engine.host '$kylin_engine_host' are valid"
elif $storage_host_vaild;then
  quit "ERROR: Checking Spark Driver Host failed, kylin.engine.host '$kylin_engine_host' is not valid',please check the logs/check-env.out for the details."
elif $engine_host_vaild;then
  quit "ERROR: Checking Spark Driver Host failed, kylin.storage.host '$kylin_storage_host' is not valid',please check the logs/check-env.out for the details."
else
  quit "ERROR: Checking Spark Driver Host failed, kylin.engine.host '$kylin_engine_host' and kylin.storage.host '$kylin_storage_host' are not valid, please check the logs/check-env.out for the details."
fi

echo "Checking Spark Driver Host succeed"