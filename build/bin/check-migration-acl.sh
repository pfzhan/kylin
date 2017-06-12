#!/bin/bash
# Kyligence Inc. License
#title=Checking ACL Migration Status

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking ACL Migration Status..."

metadataUrl=`${dir}/get-properties.sh kylin.metadata.url`
storage_type=${metadataUrl#*@}
storage_type=${storage_type%%,*}

if [[ "${storage_type}" != "hbase" ]]
then
    echo "Not HBase metadata ${metadataUrl}. Skip check."
    exit 0
fi

metadataUrl=${metadataUrl%%@*}

${dir}/kylin.sh org.apache.kylin.tool.AclTableMigrationCLI CHECK
ec=$?

[[ $ec == 2 ]] && quit "ERROR: Legacy ACL metadata detected. Please migrate ACL metadata first. Step1: run command 'bin/kylin.sh org.apache.kylin.tool.AclTableMigrationCLI MIGRATE', Step2: drop hbase tables: ${metadataUrl}_acl and ${metadataUrl}_user"
[[ $ec == 0 ]] || quit "ERROR: Unknown error. Please check full log."
