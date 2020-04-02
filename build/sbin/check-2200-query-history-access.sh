#!/bin/bash
# Kyligence Inc. License
#title=Checking Query History Accessibility

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source ${KYLIN_HOME}/sbin/init-kerberos.sh
## init Kerberos if needed
initKerberosIfNeeded

echo "Checking query history access..."

output=`${KYLIN_HOME}/sbin/bootstrap.sh io.kyligence.kap.tool.QueryHistoryAccessCLI 10`
if [[ $? == 0 ]] ; then
    echo "check query history access succeeded"
else
    quit "${output}"
fi