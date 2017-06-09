#!/bin/bash
# Kyligence Inc. License
#title=Checking Spark Availability

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

columnarEnabled=`${dir}/get-properties.sh kap.storage.columnar.start-own-spark`
[[ "${columnarEnabled}" == "true" ]] || exit 0

echo "Checking spark home..."

# check SPARK_HOME

[[ -z ${SPARK_HOME} ]] || [[ ${SPARK_HOME} == ${KYLIN_HOME}/spark ]] || echo "${CHECKENV_REPORT_PFX}`setColor 32 Important!` Current SPARK_HOME is set: ${SPARK_HOME}, please don't risk it, more info: https://kyligence.gitbooks.io/kap-manual/content/en/install/check_env.en.html"

echo "Testing spark task..."
${dir}/spark-client.sh test

echo "Checking spark executor config..."

override_file="${KYLIN_HOME}/conf/kylin.properties"

if [ ! -f ${override_file} ]; then
    echo "${override_file} not exist. Please check"
    exit 1
fi

key_executor_cores="kap.storage.columnar.spark-conf.spark.executor.cores"
key_executor_memory="kap.storage.columnar.spark-conf.spark.executor.memory"
key_executor_instance="kap.storage.columnar.spark-conf.spark.executor.instances"

mkdir -p ${KYLIN_HOME}/logs
saveFileName=${KYLIN_HOME}/logs/cluster.info
${dir}/kylin.sh io.kyligence.kap.tool.setup.KapGetClusterInfo ${saveFileName}

if [ $? != 0 ]; then
    echo "${CHECKENV_REPORT_PFX}WARN: Failed to get cluster' info, skip the spark config suggestion."
    exit 0
fi

#def constant var
yarn_available_cores=`getValueByKey availableVirtualCores ${saveFileName}`
yarn_available_memory=`getValueByKey availableMB ${saveFileName}`
spark_executor_cores=`${dir}/get-properties.sh ${key_executor_cores}`
spark_executor_memory=`${dir}/get-properties.sh ${key_executor_memory}`
spark_executor_instance=`${dir}/get-properties.sh ${key_executor_instance}`

if [ -z ${yarn_available_cores} ]; then
    echo "${CHECKENV_REPORT_PFX}WARN: Cannot get Yarn RM's cores info, skip the spark config suggestion."
    exit 0
fi

if [ -z ${yarn_available_memory} ]; then
    echo "${CHECKENV_REPORT_PFX}WARN: Cannot get Yarn RM's memory info, skip the spark config suggestion."
    exit 0
fi

echo "${key_executor_cores}=`setColor 36 ${spark_executor_cores}`"
echo "${key_executor_memory}=`setColor 36 ${spark_executor_memory}`"
echo "${CHECKENV_REPORT_PFX}The available yarn RM cores: ${yarn_available_cores}"
echo "${CHECKENV_REPORT_PFX}The available yarn RM memory: ${yarn_available_memory}M"
unit=${spark_executor_memory: -1}
unit=$(echo ${unit} | tr [a-z] [A-Z])

if [ "${unit}" == "M" ];then
    spark_executor_memory=${spark_executor_memory%?}
elif [ "${unit}" == "G" ];then
    spark_executor_memory=`expr 1024 \* ${spark_executor_memory%?}`
else
    quit "Unrecognized memory unit: ${unit} in ${spark_executor_memory} in kylin.properties";
fi

[[ ${yarn_available_cores} -gt ${spark_executor_cores} ]] || quit "In kylin.properties, ${key_executor_cores} is set to ${spark_executor_cores}, which is greater than Yarn's available cores: ${yarn_available_cores}, please correct it."
[[ ${yarn_available_memory} -gt ${spark_executor_memory} ]] || quit "In kylin.properties, ${key_executor_memory} is set to ${spark_executor_memory}M, which is more than Yarn's available memory: ${yarn_available_memory}M, please correct it."

ins1=`expr ${yarn_available_memory} / ${spark_executor_memory}`
ins2=`expr ${yarn_available_cores} / ${spark_executor_cores}`

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
    echo "${CHECKENV_REPORT_PFX}The max executor instances can be `setColor 36 ${recommend}`"
    echo "${CHECKENV_REPORT_PFX}The current executor instances is `setColor 36 ${spark_executor_instance}`"
fi
