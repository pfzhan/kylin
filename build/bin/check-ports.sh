#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

spark_client_port=`$KYLIN_HOME/bin/get-properties.sh kap.storage.columnar.spark-driver-port`
[[ -n ${spark_client_port} ]] || spark_client_port=7071
kylin_port=`$KYLIN_HOME/bin/get-properties.sh kylin.server.cluster-servers`
kylin_port=`echo ${kylin_port##*:}`

nc -z `uname -n` ${kylin_port} >/dev/null 2>&1
[[ ! $? -eq 0 ]] || quit "ERROR: port: ${kylin_port} is in use, another KAP instance is running?"
nc -z `uname -n` ${spark_client_port} >/dev/null 2>&1
[[ ! $? -eq 0 ]] || quit "ERROR: port: ${spark_client_port} is in use, please make sure it is available."