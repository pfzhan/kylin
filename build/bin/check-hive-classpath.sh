#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source $dir/find-hive-dependency.sh

jobjar=`find ${KYLIN_HOME}/lib -name '*job*.jar'`
java -cp ${hive_dependency}:${jobjar}  org.apache.kylin.common.util.ClasspathScanner  HCatInputFormat.class  >/dev/null 2>&1
[[ $? == 0 ]]         || quit "ERROR: Class HCatInputFormat is not found on hive dependency. Please check bin/find-hive-dependency.sh is setup correctly."

# check snappy
need_snappy_lib=`sh $KYLIN_HOME/bin/get-properties.sh kap.storage.columnar.page.compression`
if [[ ${need_snappy_lib} == "SNAPPY" ]]
then
    snappy_lib_count=`echo ${hive_dependency} | grep -o "snappy-java" | wc -l`
    [[ ${snappy_lib_count} != 0 ]]  || quit "ERROR: Snappy lib is not found. Please double check Snappy is installed, or disable Snappy compression."
fi
