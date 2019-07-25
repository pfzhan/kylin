#!/bin/bash
# Kyligence Inc. License
#title=Checking Zookeeper Role

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source ${KYLIN_HOME}/sbin/init-kerberos.sh
## init Kerberos if needed
initKerberosIfNeeded

echo "Checking Zookeeper role..."

zk_connect_string=`${KYLIN_HOME}/bin/get-properties.sh kylin.env.zookeeper-connect-string`

if [[ -z $zk_connect_string ]]; then
    quit "Failed: Zookeeper connect string is empty, please set 'kylin.env.zookeeper-connect-string' in {KYLIN_HOME}/conf/kylin.properties"
fi

mode=`${KYLIN_HOME}/bin/get-properties.sh kylin.server.mode`
${KYLIN_HOME}/sbin/bootstrap.sh io.kyligence.kap.tool.CuratorOperator $1

if [[ $? == 1 ]] && [[ $mode == "all" ]]; then
    quit "Failed: only one job node is allowed, please stop your running job node if it exits or wait for a while due to some latency reason"
fi