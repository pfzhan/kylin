#!/bin/bash
# Kyligence Inc. License
#title=Checking JDBC Usages

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh


metadataUrl=`${dir}/get-properties.sh kylin.metadata.url`
storage_type=${metadataUrl#*@}
storage_type=${storage_type%%,*}

if [[ "${storage_type}" != "jdbc" ]]
then
    echo "Not JDBC metadata ${metadataUrl}. Skip check."
    exit 0
fi

echo "Checking JDBC Usages"

output=`${dir}/kylin.sh io.kyligence.kap.tool.storage.KapTestJdbcCLI`
[[ $? == 0 ]] || quit "${output}"