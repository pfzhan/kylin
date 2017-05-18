#!/bin/bash
# Kyligence Inc. License
#title=Checking Spark Availability

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

columnarEnabled=`${dir}/get-properties.sh kap.storage.columnar.start-own-spark`
[[ "${columnarEnabled}" == "true" ]] || exit 0

echo "Checking spark home..."

# check SPARK_HOME

[[ -z ${SPARK_HOME} ]] || [[ ${SPARK_HOME} == ${KYLIN_HOME}/spark ]] || echo "${CHECKENV_REPORT_PFX}`setColor 32 Important!` Current SPARK_HOME is set: ${SPARK_HOME}, please don't risk it, more info: https://kyligence.gitbooks.io/kap-manual/content/zh-cn/install/check_env.cn.html"

echo "Spark home is `setColor 36 OK`!"

echo "Testing spark task..."
${dir}/spark_client.sh test

echo "Checking spark executor config..."

override_file="${KYLIN_HOME}/conf/kylin.properties"

if [ ! -f ${override_file} ]; then
    echo "${override_file} not exist. Please check"
    exit 1
fi

key_executor_cores="kap.storage.columnar.spark-conf.spark.executor.cores"
key_executor_memory="kap.storage.columnar.spark-conf.spark.executor.memory"
key_executor_instance="kap.storage.columnar.spark-conf.spark.executor.instances"

saveFileName="${KYLIN_HOME}/logs/cluster.info"
export ENABLE_CHECK_ENV=false
${dir}/kylin.sh io.kyligence.kap.tool.setup.KapGetClusterInfo ${saveFileName}

if [ $? != 0 ]; then
    echo "${CHECKENV_REPORT_PFX}WARN: Failed to get cluster' info, skip the spark config suggestion."
    exit 0
fi

#def constant var
spark_total_cores=`getValueByKey availableVirtualCores ${saveFileName}`
spark_total_memory=`getValueByKey availableMB ${saveFileName}`
spark_executor_cores=`getValueByKey ${key_executor_cores} ${override_file}`
spark_executor_memory=`getValueByKey ${key_executor_memory} ${override_file}`
spark_executor_instance=`getValueByKey ${key_executor_instance} ${override_file}`

if [ -z ${spark_total_cores} ]; then
    echo "${CHECKENV_REPORT_PFX}WARN: Cannot get Yarn RM's cores info, skip the spark config suggestion."
    exit 0
fi

if [ -z ${spark_total_memory} ]; then
    echo "${CHECKENV_REPORT_PFX}WARN: Cannot get Yarn RM's memory info, skip the spark config suggestion."
    exit 0
fi

echo "${key_executor_cores}=`setColor 36 ${spark_executor_cores}`"
echo "${key_executor_memory}=`setColor 36 ${spark_executor_memory}`"
echo "${CHECKENV_REPORT_PFX}The total yarn RM cores: `setColor 36 ${spark_total_cores}`"
echo "${CHECKENV_REPORT_PFX}The total yarn RM memory: `setColor 36 ${spark_total_memory}M`"
unit=${spark_executor_memory: -1}
unit=$(echo ${unit} | tr [a-z] [A-Z])

if [ "${unit}" == "M" ];then
    spark_executor_memory=${spark_executor_memory%?}
elif [ "${unit}" == "G" ];then
    spark_executor_memory=`expr 1024 \* ${spark_executor_memory%?}`
else
    quit "Unrecognized memory unit: ${unit} in ${spark_executor_memory} in kylin.properties";
fi

[[ ${spark_total_cores} -gt ${spark_executor_cores} ]] || quit "The executor's cores: ${spark_executor_cores} configured in kylin.properties are not correctly, even more than Yarn's total cores: ${spark_total_cores}."
[[ ${spark_total_memory} -gt ${spark_executor_memory} ]] || quit "The executor's memory: ${spark_executor_memory}M configured in kylin.properties are not correctly, even more than Yarn's total memory ${spark_total_memory}M."

ins1=`expr ${spark_total_memory} / ${spark_executor_memory}`
ins2=`expr ${spark_total_cores} / ${spark_executor_cores}`

if [ ${ins1} -lt ${ins2} ]; then
    recommend=${ins1}
else
    recommend=${ins2}
fi

if [ -z ${spark_executor_instance} ]; then
    echo "${CHECKENV_REPORT_PFX}`setColor 31 WARN:` ${key_executor_instance} is not set."
elif [ ${spark_executor_instance} -gt ${recommend} ]; then
    quit "The executor's instances: ${spark_executor_instance} configured in kylin.properties shouldn't beyond maximum: ${recommend} in theory."
elif [ `expr ${recommend} / ${spark_executor_instance}` -gt 5 ]; then
    echo "${CHECKENV_REPORT_PFX}`setColor 31 WARN:` The executor's instances: ${spark_executor_instance} in kylin.properties is set too few, the maximum could be ${recommend}"
else
    echo "${CHECKENV_REPORT_PFX}The max executor's instances can be: ${recommend}"
fi
