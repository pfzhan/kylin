#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/find-hadoop-conf-dir.sh

echo "Checking HADOOP_CONF_DIR..."

export ENABLE_CHECK_ENV=false
${dir}/kylin.sh io.kyligence.kap.engine.mr.tool.CheckHadoopConfDir --config "${kylin_hadoop_conf_dir}" "${kylin_hadoop_conf_dir}"

[[ $? == 0 ]] || quit "ERROR: Check HADOOP_CONF_DIR failed. Please correct hadoop configurations."
