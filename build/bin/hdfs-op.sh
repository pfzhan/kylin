#!/bin/bash

# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/find-hadoop-conf-dir.sh

WORKING_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.env.hdfs-working-dir`
ENABLE_FS_SEPARATE=`$KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.separate-fs-enable | tr '[A-Z]' '[a-z]'`

if [ -z "${kylin_hadoop_conf_dir}" ]; then
    hadoop_conf_param=
else
    hadoop_conf_param="--config ${kylin_hadoop_conf_dir}"
fi

if [ "$1" == "put" ];
then
    hdfs_test_dir=
    hdfs_test_file="$2"
    [[ ! -f ./${hdfs_test_file} ]] || rm -f ./${hdfs_test_file}
    touch ./${hdfs_test_file}
    if [ -n ${ENABLE_FS_SEPARATE} ] && [ "${ENABLE_FS_SEPARATE}" == "true" ]; then
        remote_working_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.file-system`${WORKING_DIR#*://}
        hdfs_test_dir=${remote_working_dir}/${hdfs_test_file}
    else
        hdfs_test_dir=${WORKING_DIR}/${hdfs_test_file}
    fi
    echo "${hdfs_test_dir}" >> ${hdfs_test_file}
    hadoop ${hadoop_conf_param} fs -put -f ./${hdfs_test_file} ${hdfs_test_dir} || quit "ERROR: Have no permission to create/modify file in working directory: ${WORKING_DIR}"
    exit 0
fi

if [ "$1" == "rm" ]
then
    hdfs_test_file="$2"
    if [ -n ${ENABLE_FS_SEPARATE} ] && [ "${ENABLE_FS_SEPARATE}" == "true" ]; then
        remote_working_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.file-system`${WORKING_DIR#*://}
        hdfs_test_dir=${remote_working_dir}/${hdfs_test_file}
    else
        hdfs_test_dir=${WORKING_DIR}/${hdfs_test_file}
    fi
    [[ ! -f ./${hdfs_test_file} ]] || rm -f ./${hdfs_test_file}
    hadoop ${hadoop_conf_param} fs -rm -skipTrash ${hdfs_test_dir}
    exit 0
fi