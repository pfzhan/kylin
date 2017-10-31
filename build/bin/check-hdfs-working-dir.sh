#!/bin/bash
# Kyligence Inc. License
#title=Checking Permission of HDFS Working Dir

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

## ${dir} assigned to $KYLIN_HOME/bin in header.sh
source ${dir}/find-hadoop-conf-dir.sh

echo "Checking HDFS working dir..."

RANDNAME=chkenv__${RANDOM}
WORKING_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.env.hdfs-working-dir`
ENABLE_FS_SEPARATE=`$KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.separate-fs-enable | tr '[A-Z]' '[a-z]'`
TEST_FILE=${WORKING_DIR}/${RANDNAME}

if [ -z "${kylin_hadoop_conf_dir}" ]; then
    hadoop_conf_param=
else
    hadoop_conf_param="--config ${kylin_hadoop_conf_dir}"
fi

# test local hdfs
## in read-write separation mode this is write cluster
hadoop ${hadoop_conf_param} fs -test -d ${WORKING_DIR} || quit "ERROR: Please create local working directory '${WORKING_DIR}' and grant access permission to current user."

# test if kylin user (current user) has write permission to working directory
touch ./${RANDNAME}
hadoop ${hadoop_conf_param} fs -put -f ./${RANDNAME} ${TEST_FILE} || quit "ERROR: Have no permission to create/modify file in local working directory '${WORKING_DIR}'. Please grant permission to current user."

rm -f ./${RANDNAME}
hadoop ${hadoop_conf_param} fs -rm -skipTrash ${TEST_FILE}

# test read hdfs if necessary
# in read-write separation mode this is read cluster
if [ -n ${ENABLE_FS_SEPARATE} ] && [ "${ENABLE_FS_SEPARATE}" == "true" ]; then
    #convert local working dir to read cluster working dir
    read_working_dir=$(${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.setup.KapGetPathWithoutSchemeAndAuthorityCLI ${WORKING_DIR}| grep -v 'Usage'|tail -1)
    read_working_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.file-system`${read_working_dir}
    hadoop ${hadoop_conf_param} fs -test -d ${read_working_dir} || quit "ERROR: Please create read cluster working directory '${read_working_dir}' and grant access permission to current user."

    touch ./${RANDNAME}
    TEST_FILE=${read_working_dir}/${RANDNAME}
    hadoop ${hadoop_conf_param} fs -put -f ./${RANDNAME} ${TEST_FILE} || quit "ERROR: Have no permission to create/modify file in read cluster working directory '${read_working_dir}'. Please grant permission to current user."

    rm -f ./${RANDNAME}
    hadoop ${hadoop_conf_param} fs -rm -skipTrash ${TEST_FILE}
fi
