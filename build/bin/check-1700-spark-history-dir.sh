#!/bin/bash
# Kyligence Inc. License
#title=Checking Spark History Dir

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
source ${dir}/prepare-hadoop-conf-dir.sh
source ${dir}/init-kerberos.sh

## init Kerberos if needed
initKerberosIfNeeded

if [ -z "${kylin_hadoop_conf_dir}" ]; then
    hadoop_conf_param=
else
    hadoop_conf_param="--config ${kylin_hadoop_conf_dir}"
fi

spark_log_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.engine.spark-conf.spark.history.fs.logDirectory`
spark_eventlog_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.engine.spark-conf.spark.eventLog.dir`

hadoop ${hadoop_conf_param} fs -test -d ${spark_log_dir} || hadoop ${hadoop_conf_param} fs -mkdir ${spark_log_dir}

# test write permission
RANDNAME=chkenv__${RANDOM}
TEST_FILE=${spark_log_dir}/${RANDNAME}

touch ./${RANDNAME}
hadoop ${hadoop_conf_param} fs -put -f ./${RANDNAME} ${TEST_FILE} || quit "ERROR: Have no permission to create/modify file in spark history log directory '${spark_log_dir}'. Please grant permission to current user."

rm -f ./${RANDNAME}
hadoop ${hadoop_conf_param} fs -rm -skipTrash ${TEST_FILE}

if [[ ${spark_log_dir} == ${spark_eventlog_dir} ]]; then
    exit 0
fi

hadoop ${hadoop_conf_param} fs -test -d ${spark_eventlog_dir} || hadoop ${hadoop_conf_param} fs -mkdir ${spark_log_dir}

# test write permission
RANDNAME=chkenv__${RANDOM}
TEST_FILE=${spark_eventlog_dir}/${RANDNAME}

touch ./${RANDNAME}
hadoop ${hadoop_conf_param} fs -put -f ./${RANDNAME} ${TEST_FILE} || quit "ERROR: Have no permission to create/modify file in spark history event log directory '${spark_eventlog_dir}'. Please grant permission to current user."

rm -f ./${RANDNAME}
hadoop ${hadoop_conf_param} fs -rm -skipTrash ${TEST_FILE}