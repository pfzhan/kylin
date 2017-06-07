#!/bin/bash
# Kyligence Inc. License
#title=Checking Permission of HBase's Table

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking HBase create table..."

metadataUrl=`${dir}/get-properties.sh kylin.metadata.url`
storage_type=${metadataUrl#*@}
storage_type=${storage_type%%,*}

if [[ "${storage_type}" != "hbase" ]]
then
    echo "Not HBase metadata ${metadataUrl}. Skip check."
    exit 0
fi


TESTTABLE=chkenv__${RANDOM}

a=$(echo "create '$TESTTABLE', 'f1'" | hbase shell 2>&1)
echo "$a"

[[ $a == *"0 row(s) in"*seconds* ]]               || quit "ERROR: Cannot create HTable '$TESTTABLE'. Please check HBase permissions and verify if current user can create HTable in 'hbase shell'."

a=$(echo "disable '$TESTTABLE'" | hbase shell 2>&1)
echo "$a"
[[ $a == *"0 row(s) in"*seconds* ]]               || echo "${CHECKENV_REPORT_PFX}WARN: Cannot disable HTable '$TESTTABLE'."

a=$(echo "drop '$TESTTABLE'" | hbase shell 2>&1)
echo "$a"
[[ $a == *"0 row(s) in"*seconds* ]]               || echo "${CHECKENV_REPORT_PFX}WARN: Cannot drop HTable '$TESTTABLE'."

