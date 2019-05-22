#!/bin/bash
# Kyligence Inc. License
#title=Checking Kerberos

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
## ${dir} assigned to $KYLIN_HOME/bin in header.sh
source ${dir}/init-kerberos.sh

echo "Checking Kerberos..."

KAP_KERBEROS_ENABLED=`$KYLIN_HOME/bin/get-properties.sh kap.kerberos.enabled`

if [[ "${KAP_KERBEROS_ENABLED}" == "true" ]]
then
    initKerberosIfNeeded
else
    echo "KAP_KERBEROS_ENABLED is ${KAP_KERBEROS_ENABLED}. Skip check."
    exit 3
fi