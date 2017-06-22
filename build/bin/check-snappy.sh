#!/bin/bash
# Kyligence Inc. License
#title=Checking Snappy Availability

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking Snappy..."

mkdir -p ${KYLIN_HOME}/logs/tmp
input_file=${KYLIN_HOME}/logs/tmp/snappy_test_input
[[ ! -f ${input_file} ]] || rm -f ${input_file}
echo "Hello Snappy" >> ${input_file};
source ${dir}/hdfs-op.sh put ${input_file}
source ${dir}/hdfs-op.sh mkdir snappy_test_output

${dir}/kylin.sh io.kyligence.kap.tool.mr.KapMRJobCLI ${TARGET_HDFS_FILE} ${TARGET_HDFS_DIR}
[[ $? == 0 ]] || quit "Test MR job with SnappyCodec failed, please check the full log for more details."
${dir}/hdfs-op.sh rm ${input_file}
${dir}/hdfs-op.sh rm snappy_test_output

