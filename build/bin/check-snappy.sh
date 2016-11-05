#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking Snappy..."

if [ -z "${hive_dependency}" ]; then
    source ${dir}/find-hive-dependency.sh
fi

# check snappy
need_snappy_lib=`sh ${dir}/get-properties.sh kap.storage.columnar.page.compression`
if [[ ${need_snappy_lib} == "SNAPPY" ]]
then
    snappy_lib_count=`echo ${hive_dependency} | grep -o "snappy-java" | wc -l`
    [[ ${snappy_lib_count} != 0 ]]  || quit "ERROR: Snappy lib is not found. Please double check Snappy is installed, or disable Snappy compression."
fi
