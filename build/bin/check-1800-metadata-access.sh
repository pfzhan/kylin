#!/bin/bash
# Kyligence Inc. License
#title=Checking Metadata Accessibility

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source ${dir}/init-kerberos.sh
## init Kerberos if needed
initKerberosIfNeeded

echo "Checking Metadata Accessibility..."

output=`${dir}/kylin.sh io.kyligence.kap.tool.metadata.CheckMetadataAccessCLI 10`
[[ $? == 0 ]] || quit "${output}"