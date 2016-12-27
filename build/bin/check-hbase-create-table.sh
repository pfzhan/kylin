#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking HBase create table..."

TESTTABLE=chkenv__${RANDOM}

a=$(echo "create '$TESTTABLE', 'f1'" | hbase shell 2>&1)
echo "$a"
[[ $a == *"0 row(s) in"*seconds* ]]               || quit "ERROR: Cannot create HTable '$TESTTABLE'. Please check HBase permission."

a=$(echo "disable '$TESTTABLE'" | hbase shell 2>&1)
echo "$a"
[[ $a == *"0 row(s) in"*seconds* ]]               || echo "WARN: Cannot disable HTable '$TESTTABLE'."

a=$(echo "drop '$TESTTABLE'" | hbase shell 2>&1)
echo "$a"
[[ $a == *"0 row(s) in"*seconds* ]]               || echo "WARN: Cannot drop HTable '$TESTTABLE'."

