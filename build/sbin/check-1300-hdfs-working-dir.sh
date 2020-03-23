#!/bin/bash
# Kyligence Inc. License
#title=Checking Permission of HDFS Working Dir

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source ${KYLIN_HOME}/sbin/find-working-dir.sh
source ${KYLIN_HOME}/sbin/init-kerberos.sh

## init Kerberos if needed
initKerberosIfNeeded

echo "Checking HDFS working dir..."

RANDNAME=chkenv__${RANDOM}
TEST_FILE=${WORKING_DIR}/${RANDNAME}

# test local hdfs
## in read-write separation mode this is write cluster
hadoop ${hadoop_conf_param} fs -test -d ${WORKING_DIR} || quit "ERROR: Please create working directory '${WORKING_DIR}' and grant access permission to current user."

# test if kylin user (current user) has write permission to working directory
touch ./${RANDNAME}
hadoop ${hadoop_conf_param} fs -put -f ./${RANDNAME} ${TEST_FILE} || quit "ERROR: Have no permission to create/modify file in working directory '${WORKING_DIR}'. Please grant permission to current user."

rm -f ./${RANDNAME}
hadoop ${hadoop_conf_param} fs -rm -skipTrash ${TEST_FILE}
