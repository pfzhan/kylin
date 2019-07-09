#!/bin/bash
# Kyligence Inc. License
#title=Checking Zookeeper Role

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source ${KYLIN_HOME}/sbin/init-kerberos.sh
## init Kerberos if needed
initKerberosIfNeeded

echo "Checking Zookeeper role..."

${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.CuratorOperator $1 2>/dev/null

if [[ $? == 1 ]]; then
    quit "Failed, only one job node is allowed"
fi