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

#title=Checking Spark Queue

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
source ${KYLIN_HOME}/sbin/init-kerberos.sh

## init Kerberos if needed
initKerberosIfNeeded

echo "Checking Spark Queue..."

kylin_storage_queue=`$KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.spark-conf.spark.yarn.queue`
kylin_engine_queue=`$KYLIN_HOME/bin/get-properties.sh kylin.engine.spark-conf.spark.yarn.queue`
kylin_principal=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.principal`

# list all the queue
queue_list_str=`mapred queue -showacls | awk 'NR>4  {print $1}'`
queue_arr=($queue_list_str)

# flag variable to indicate whether the queue is valid or not
storage_vaild=false
engine_vaild=false
for queue in "${queue_arr[@]}"
do
  # in case of 'xxx.queue'
  queue=${queue##*.}
  if [ $queue = $kylin_storage_queue ];then
    storage_vaild=true
  fi
  if [ $queue = $kylin_engine_queue ];then
    engine_vaild=true
  fi
done

if $storage_vaild && $engine_vaild;then
  echo "'$kylin_storage_queue' and '$kylin_engine_queue' exist"
elif $storage_vaild;then
  quit "ERROR: Checking Spark Queue failed, please check the queue setting '$kylin_engine_queue' in kylin.properties"
elif $engine_vaild;then
  quit "ERROR: Checking Spark Queue failed, please check the queue setting '$kylin_storage_queue' in kylin.properties"
else
  quit "ERROR: Checking Spark Queue failed, please check the queue setting '$kylin_storage_queue' and '$kylin_engine_queue' in kylin.properties"
fi

# check SUBMIT permission
submit_reg='.*SUBMIT_APPLICATIONS.*'
storage_submit_info=`mapred queue -showacls | awk '$1=="'$kylin_storage_queue'" {print $2}'`
engine_submit_info=`mapred queue -showacls | awk '$1=="'$kylin_engine_queue'" {print $2}'`

if [[ "$storage_submit_info" =~ $submit_reg ]] && [[ "$engine_submit_info" =~ $submit_reg ]];then
  echo "'$kylin_principal' can submit to '$kylin_storage_queue' and '$kylin_engine_queue'"
elif [[ "$storage_submit_info" =~ $submit_reg ]];then
  quit "ERROR: Checking Spark Queue failed, '$kylin_principal' can not submit task to '$kylin_engine_queue'"
elif [[ "$engine_submit_info" =~ $submit_reg ]];then
  quit "ERROR: Checking Spark Queue failed, '$kylin_principal' can not submit task to  '$kylin_storage_queue'"
else
  quit "ERROR: Checking Spark Queue failed, '$kylin_principal' can not submit task to  '$kylin_storage_queue' and '$kylin_engine_queue'"
fi

# check the queue is running or not
running_str=running
storage_running_info=`mapred queue -info $kylin_storage_queue | awk '$2=="State" {print $4}'`
engine_running_info=`mapred queue -info $kylin_engine_queue | awk '$2=="State" {print $4}'`

if [ $storage_running_info = $running_str ] && [ $engine_running_info = $running_str ];then
  echo "'$kylin_storage_queue' and '$kylin_engine_queue' are running"
elif [ $storage_running_info = $running_str ];then
  quit "ERROR: Checking Spark Queue failed, '$kylin_engine_queue' is not running"
elif [ $engine_running_info = $running_str ];then
  quit "ERROR: Checking Spark Queue failed, '$kylin_storage_queue' is not running"
else
  quit "ERROR: Checking Spark Queue failed, '$kylin_storage_queue' and '$kylin_engine_queue' are not running"
fi

echo "Checking Spark Queue succeed"