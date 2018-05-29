#!/bin/bash
# Kyligence Inc. License
#title=Checking Hive Classpath

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking Hive classpath..."

if [ -z "${hive_dependency}" ]; then
    ## ${dir} assigned to $KYLIN_HOME/bin in header.sh
    source ${dir}/find-hive-dependency.sh
fi

jobjar=`find ${KYLIN_HOME}/lib -name '*job*.jar'`
$JAVA -cp ${hive_dependency}:${jobjar}  ClasspathScanner  HCatInputFormat.class

[[ $? == 0 ]]         || quit "ERROR: Class HCatInputFormat is not found on hive dependency. Please check 'bin/find-hive-dependency.sh' reports the correct Hive classpath."
