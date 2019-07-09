#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

## ${dir} assigned to $KYLIN_HOME/bin in header.sh
source ${dir}/prepare-hadoop-conf-dir.sh

cd ${KYLIN_HOME}/tool/ssb/data

if [ -z "${kylin_hadoop_conf_dir}" ]; then
    hadoop_conf_param=
else
    hadoop_conf_param="--config ${kylin_hadoop_conf_dir}"
fi

if [ -z "$1" ]; then
    hdfs_tmp_dir=/tmp/kylin
else
    hdfs_tmp_dir=$1
fi

echo "Loading sample data into HDFS tmp path: ${hdfs_tmp_dir}/sample_cube/data"
hadoop ${hadoop_conf_param} fs -mkdir -p ${hdfs_tmp_dir}/sample_cube/data
if [ $? != 0 ]
then
    quit "Failed to create ${hdfs_tmp_dir}/sample_cube/data. Please make sure the user has right to access ${hdfs_tmp_dir}/sample_cube/data or usage: sample.sh hdfs_tmp_dir"
fi

hadoop ${hadoop_conf_param} fs -put * ${hdfs_tmp_dir}/sample_cube/data/

hive_client_mode=`bash ${KYLIN_HOME}/bin/get-properties.sh kylin.source.hive.client`
sample_database=SSB
echo "Going to create sample tables in hive to database "$sample_database" by "$hive_client_mode

if [ "${hive_client_mode}" == "beeline" ]
then
    beeline_params=`bash ${KYLIN_HOME}/bin/get-properties.sh kylin.source.hive.beeline-params`
    beeline ${hive_conf_properties} ${beeline_params} -e "CREATE DATABASE IF NOT EXISTS "$sample_database
    hive2_url=`expr match "${beeline_params}" '.*\(hive2:.*:[0-9]\{4,6\}\/\)'`
    if [ -z ${hive2_url} ]; then
        hive2_url=`expr match "${beeline_params}" '.*\(hive2:.*:[0-9]\{4,6\}\)'`
        beeline_params=${beeline_params/${hive2_url}/${hive2_url}/${sample_database}}
    else
        beeline_params=${beeline_params/${hive2_url}/${hive2_url}${sample_database}}
    fi

    beeline ${hive_conf_properties} --hivevar hdfs_tmp_dir=${hdfs_tmp_dir} ${beeline_params} -f ${KYLIN_HOME}/tool/ssb/create_sample_ssb_tables.sql  || { exit 1; }
else
    hive ${hive_conf_properties} -e "CREATE DATABASE IF NOT EXISTS "$sample_database
    hive ${hive_conf_properties} --hivevar hdfs_tmp_dir=${hdfs_tmp_dir} --database $sample_database -f ${KYLIN_HOME}/tool/ssb/create_sample_ssb_tables.sql  || { exit 1; }
fi

echo "Sample hive tables are created successfully"
