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
    local_file="$2"
    if [ -n ${ENABLE_FS_SEPARATE} ] && [ "${ENABLE_FS_SEPARATE}" == "true" ]; then
        remote_working_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.file-system`${WORKING_DIR#*://}
        tmp_dir=${remote_working_dir}
    else
        tmp_dir=${WORKING_DIR}
    fi

    hdfs_full_file=${tmp_dir}/`basename ${local_file}`
    hadoop ${hadoop_conf_param} fs -put -f ${local_file} ${hdfs_full_file} || quit "ERROR: Have no permission to create/modify file in working directory: ${WORKING_DIR}"
    export TARGET_HDFS_FILE=${hdfs_full_file}
fi

if [ "$1" == "rm" ]
then
    local_file="$2"
    [[ -n ${local_file} ]] || exit 1
    if [ -n ${ENABLE_FS_SEPARATE} ] && [ "${ENABLE_FS_SEPARATE}" == "true" ]; then
        remote_working_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.file-system`${WORKING_DIR#*://}
        tmp_dir=${remote_working_dir}
    else
        tmp_dir=${WORKING_DIR}
    fi
    [[ ! -f ${local_file} ]] || rm -f ${local_file}

    remote_input_dir=${tmp_dir}/`basename ${local_file}`
    hadoop ${hadoop_conf_param} fs -rm -r -skipTrash ${remote_input_dir}
    exit 0
fi

if [ "$1" == "mkdir" ]
then
    if [ -n ${ENABLE_FS_SEPARATE} ] && [ "${ENABLE_FS_SEPARATE}" == "true" ]; then
        remote_working_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.file-system`${WORKING_DIR#*://}
        hdfs_test_dir=${remote_working_dir}/"$2"
    else
        hdfs_test_dir=${WORKING_DIR}/"$2"
    fi
    hadoop ${hadoop_conf_param} fs -mkdir -p ${hdfs_test_dir}
    export TARGET_HDFS_DIR=${hdfs_test_dir}
fi