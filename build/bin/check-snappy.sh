#!/bin/bash
# Kyligence Inc. License
#title=Checking Snappy Availability

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source ${dir}/find-working-dir.sh

echo "Checking Snappy..."

local_input_dir=${KYLIN_HOME}/logs/tmp
local_input_file=snappy_test_input
hdfs_output_dir=snappy_test_output
full_input_file=${local_input_dir}/${local_input_file}

mkdir -p ${local_input_dir}
[[ ! -f ${full_input_file} ]] || rm -f ${full_input_file}

echo "Hello Snappy" >> ${full_input_file};

hadoop ${KAP_HADOOP_PARAM} fs -put -f ${full_input_file} ${KAP_WORKING_DIR}
hadoop ${KAP_HADOOP_PARAM} fs -mkdir ${KAP_WORKING_DIR}/${hdfs_output_dir}

${dir}/kylin.sh io.kyligence.kap.tool.mr.KapMRJobCLI ${KAP_WORKING_DIR}/${local_input_file} ${KAP_WORKING_DIR}/${hdfs_output_dir}
[[ $? == 0 ]] || quit "Test MR job with SnappyCodec failed, please check the full log for more details."

hadoop ${KAP_HADOOP_PARAM} fs -rm -r -skipTrash ${KAP_WORKING_DIR}/${local_input_file}
hadoop ${KAP_HADOOP_PARAM} fs -rm -r -skipTrash ${KAP_WORKING_DIR}/${hdfs_output_dir}
rm -rf ${full_input_file}

