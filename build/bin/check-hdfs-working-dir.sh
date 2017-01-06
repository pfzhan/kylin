#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking HDFS working dir..."

RANDNAME=chkenv__${RANDOM}
WORKING_DIR=`bash $KYLIN_HOME/bin/get-properties.sh kylin.env.hdfs-working-dir`
TEST_FILE=${WORKING_DIR}/${RANDNAME}

$(hadoop fs -test -d ${WORKING_DIR}) || quit "ERROR: Please create working directory '${WORKING_DIR}' and grant access permission."

# test if kylin user (current user) has write permission to working directory
touch ./${RANDNAME}
hadoop fs -put -f ./${RANDNAME} ${TEST_FILE} || quit "ERROR: Have no permission to create/modify file in working directory: ${WORKING_DIR}"

rm -f ./${RANDNAME}
hadoop fs -rm -skipTrash ${TEST_FILE}
