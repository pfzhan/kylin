#!/bin/bash
# Kyligence Inc. License
#title=Checking Snappy Availability

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking Snappy..."

if [ -z "${hive_dependency}" ]; then
    source ${dir}/find-hive-dependency.sh
fi

# check snappy
need_snappy_lib=`${dir}/get-properties.sh kap.storage.columnar.page-compression`
if [[ ${need_snappy_lib} == "SNAPPY" ]]
then
    snappy_lib_count=`echo ${hive_dependency} | grep -o "snappy-java" | wc -l`
    [[ ${snappy_lib_count} != 0 ]]  || quit "ERROR: Snappy lib is not found. Please double check Snappy is installed, or disable Snappy compression."
fi

input_file=${KYLIN_HOME}/logs/snappy_test_input
[[ ! -f ${input_file} ]] || rm -f ${input_file}
echo "Hello Snappy" >> ${input_file};
source ${dir}/hdfs-op.sh put ${input_file}
source ${dir}/hdfs-op.sh mkdir snappy_test_output

export ENABLE_CHECK_ENV=false
${dir}/kylin.sh io.kyligence.kap.tool.mr.KapMRJobCLI ${TARGET_HDFS_FILE} ${TARGET_HDFS_DIR}
[[ $? == 0 ]] || quit "Test MR job with SnappyCodec failed, please check the full log for more details."
${dir}/hdfs-op.sh rm ${input_file}
${dir}/hdfs-op.sh rm snappy_test_output

