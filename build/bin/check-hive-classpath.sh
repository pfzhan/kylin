#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking Hive classpath..."

if [ -z "${hive_dependency}" ]; then
    source ${dir}/find-hive-dependency.sh
fi

jobjar=`find ${KYLIN_HOME}/lib -name '*job*.jar'`
java -cp ${hive_dependency}:${jobjar}  org.apache.kylin.common.util.ClasspathScanner  HCatInputFormat.class
[[ $? == 0 ]]         || quit "ERROR: Class HCatInputFormat is not found on hive dependency. Please check bin/find-hive-dependency.sh is setup correctly."
