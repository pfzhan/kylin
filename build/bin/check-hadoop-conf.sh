#!/bin/bash
# Kyligence Inc. License
#title=Checking Hadoop Configuration

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

## ${dir} assigned to $KYLIN_HOME/bin in header.sh
source ${dir}/find-hadoop-conf-dir.sh

echo "Checking hadoop conf dir..."

[[ -z "${kylin_hadoop_conf_dir}" ]] && quit "ERROR: Failed to find Hadoop config dir, please set HADOOP_CONF_DIR."


${dir}/kylin.sh io.kyligence.kap.engine.mr.tool.CheckHadoopConfDir "${kylin_hadoop_conf_dir}"

# CheckHadoopConfDir will print the last error message
[[ $? == 0 ]] || quit "ERROR: Check HADOOP_CONF_DIR failed. Please correct hadoop configurations."
