#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

## ${dir} assigned to $KYLIN_HOME/bin in header.sh
source ${dir}/find-hadoop-conf-dir.sh

WORKING_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.env.hdfs-working-dir`
ENABLE_FS_SEPARATE=`$KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.separate-fs-enable | tr '[A-Z]' '[a-z]'`

if [ -z "${kylin_hadoop_conf_dir}" ]; then
    hadoop_conf_param=
else
    hadoop_conf_param="--config ${kylin_hadoop_conf_dir}"
fi

if [ -n ${ENABLE_FS_SEPARATE} ] && [ "${ENABLE_FS_SEPARATE}" == "true" ]; then
        WORKING_DIR=$(${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.setup.KapGetPathWithoutSchemeAndAuthorityCLI ${WORKING_DIR}| grep -v 'Usage'|tail -1)
        final_working_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.file-system`${WORKING_DIR}
    else
        final_working_dir=${WORKING_DIR}
fi

export KAP_HADOOP_PARAM=${hadoop_conf_param}
export KAP_WORKING_DIR=${final_working_dir}