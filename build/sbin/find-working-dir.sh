#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
source ${KYLIN_HOME}/sbin/init-kerberos.sh

## init Kerberos if needed
initKerberosIfNeeded

source ${KYLIN_HOME}/sbin/prepare-hadoop-conf-dir.sh

WORKING_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.env.hdfs-working-dir`

if [ -z "${kylin_hadoop_conf_dir}" ]; then
    hadoop_conf_param=
else
    hadoop_conf_param="--config ${kylin_hadoop_conf_dir}"
fi

final_working_dir=${WORKING_DIR}

export KAP_HADOOP_PARAM=${hadoop_conf_param}
export KAP_WORKING_DIR=${final_working_dir}