#!/bin/bash
# Kyligence Inc. License
#title=Checking Ports Availability

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

[[ -z $KYLIN_CONF ]] || quit "KYLIN_CONF should not be set. Please do: export KYLIN_CONF="

kylin_port=`$KYLIN_HOME/bin/get-properties.sh server.port`
if [[ -z ${kylin_port} ]]; then
    kylin_port=7070
fi

kylin_port_in_use=`netstat -tlpn | grep "\b${kylin_port}\b"`
[[ -z ${kylin_port_in_use} ]] || quit "ERROR: Port ${kylin_port} is in use, another Kyligence Enterprise server is running?"
