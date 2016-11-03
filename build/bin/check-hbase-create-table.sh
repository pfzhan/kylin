#!/bin/bash
# Kyligence Inc. License

dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

function quit {
	echo "$@"
	exit 1
}

TESTTABLE=__test__table__

a=$(echo "create '$TESTTABLE', 'f1'" | hbase shell 2>&1)
[[ $a == *"0 row(s) in"*seconds* ]]               || quit "ERROR: Cannot create HTable '$TESTTABLE'. Please check HBase permission."

a=$(echo "disable '$TESTTABLE'" | hbase shell 2>&1)
[[ $a == *"0 row(s) in"*seconds* ]]               || echo "WARN: Cannot disable HTable '$TESTTABLE'."

a=$(echo "drop '$TESTTABLE'" | hbase shell 2>&1)
[[ $a == *"0 row(s) in"*seconds* ]]               || echo "WARN: Cannot drop HTable '$TESTTABLE'."

