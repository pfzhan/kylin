#!/bin/bash
# Kyligence Inc. License
#title=Checking Zookeeper Role

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source ${dir}/init-kerberos.sh
## init Kerberos if needed
initKerberosIfNeeded

echo "Checking Zookeeper role..."

is_job=`${dir}/kylin.sh io.kyligence.kap.tool.CuratorOperator $1 2>/dev/null`

if [[ ${is_job} == "true" ]]; then
    quit "Failed, only one job node is allowed"
fi