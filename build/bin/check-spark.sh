#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking SPARK_HOME..."

# check SPARK_HOME

[[ ${SPARK_HOME} == '' ]] || [[ ${SPARK_HOME} == ${KYLIN_HOME}/spark ]] || quit "Current SPARK_HOME: ${SPARK_HOME} should set to ${KYLIN_HOME}/spark!"