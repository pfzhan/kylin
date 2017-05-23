#!/bin/bash
# Kyligence Inc. License
#title=Checking Ports Availability

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

[[ -z $KYLIN_CONF ]] || quit "KYLIN_CONF should not be set. Please do: export KYLIN_CONF="

spark_client_port=`$KYLIN_HOME/bin/get-properties.sh kap.storage.columnar.spark-driver-port`
[[ -n ${spark_client_port} ]] || spark_client_port=7071
kylin_port=`$KYLIN_HOME/bin/get-properties.sh kylin.server.cluster-servers`
kylin_port=`echo ${kylin_port##*:}`

kylin_port_in_use=`netstat -tlpn | grep "\b${kylin_port}\b"`
[[ -z ${kylin_port_in_use} ]] || quit "ERROR: Port ${kylin_port} is in use, another KAP instance is running?"

spark_client_port_in_use=`netstat -tlpn | grep "\b${spark_client_port}\b"`
[[ -z ${spark_client_port_in_use} ]] || quit "ERROR: Port ${spark_client_port} is in use, spark_client is already running?"