#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/find-hadoop-conf-dir.sh

echo "Checking hadoop conf dir..."

[[ -z "${kylin_hadoop_conf_dir}" ]] && quit "ERROR: Failed to find Hadoop config dir, please set HADOOP_CONF_DIR."

export ENABLE_CHECK_ENV=false
${dir}/kylin.sh io.kyligence.kap.engine.mr.tool.CheckHadoopConfDir "${kylin_hadoop_conf_dir}"

# CheckHadoopConfDir will print the last error message
[[ $? == 0 ]] || exit 1
