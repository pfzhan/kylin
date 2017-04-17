#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

columnarEnabled=`${dir}/get-properties.sh kap.storage.columnar.start-own-spark`
[[ "${columnarEnabled}" == "true" ]] || exit 0

function getValueByKey()
{
    while read line;
    do key=${line%=*};val=${line#*=};
    if [ "${key}" == "$1" ]; then
        echo $val;break;
    fi
    done<$2
}

echo "Checking spark home..."

# check SPARK_HOME

[[ ${SPARK_HOME} == '' ]] || [[ ${SPARK_HOME} == ${KYLIN_HOME}/spark ]] || quit "Current SPARK_HOME: ${SPARK_HOME} should set to ${KYLIN_HOME}/spark!"

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
${dir}/kylin.sh io.kyligence.kap.tool.setup.KapGetClusterInfo ${saveFileName} &> /dev/null
[[ $? == 0 ]] || quit "ERROR: failed to get cluster cores and memory!"

#def constant var
spark_total_cores=`getValueByKey availableVirtualCores ${saveFileName}`
spark_total_memory=`getValueByKey availableMB ${saveFileName}`
spark_executor_cores=`getValueByKey ${key_executor_cores} ${override_file}`
spark_executor_memory=`getValueByKey ${key_executor_memory} ${override_file}`
spark_executor_instance=`getValueByKey ${key_executor_instance} ${override_file}`

if [ -z ${spark_total_cores} ]; then
    echo ">   : Can not get Yarn RM's number of cores"
    exit 0
fi

if [ -z ${spark_total_memory} ]; then
    echo ">   : Can not get Yarn RM's size of memory"
    exit 0
fi

echo "${key_executor_cores}=`setColor 36 ${spark_executor_cores}`"
echo "${key_executor_memory}=`setColor 36 ${spark_executor_memory}`"
echo ">   The total yarn RM cores: `setColor 36 ${spark_total_cores}`"
echo ">   The total yarn RM memory: `setColor 36 ${spark_total_memory}M`"
unit=${spark_executor_memory: -1}
unit=$(echo ${unit} | tr [a-z] [A-Z])

if [ "${unit}" == "M" ];then
    spark_executor_memory=${spark_executor_memory%?}
elif [ "${unit}" == "G" ];then
    spark_executor_memory=`expr 1024 \* ${spark_executor_memory%?}`
else
    quit "ERROR: Unrecognized memory unit in ${spark_executor_memory}";
fi

[[ ${spark_total_cores} -gt ${spark_executor_cores} ]] || quit "ERROR: Spark executor's cores(`setColor 36 ${spark_executor_cores}`) are too many, greater than spark total cores(`setColor 36 ${spark_total_cores}`)!"
[[ ${spark_total_memory} -gt ${spark_executor_memory} ]] || quit "ERROR: Spark executor's memory(`setColor 36 ${spark_executor_memory}M`) is too much, more than spark total memory(`setColor 36 ${spark_total_memory}M`)!"

ins1=`expr ${spark_total_memory} / ${spark_executor_memory}`
ins2=`expr ${spark_total_cores} / ${spark_executor_cores}`

if [ ${ins1} -lt ${ins2} ]; then
    recommend=${ins1}
else
    recommend=${ins2}
fi

if [ -z ${spark_executor_instance} ]; then
    echo ">   `setColor 31 WARN:` ${key_executor_instance} is not set."
elif [ ${spark_executor_instance} -gt ${recommend} ]; then
    quit "ERROR: The configured spark executor instance(`setColor 31 ${spark_executor_instance}`) should not be greater than the max(`setColor 36 ${recommend}`)."
elif [ `expr ${recommend} / ${spark_executor_instance}` -gt 5 ]; then
    echo ">   `setColor 31 WARN:` The configured spark executor instance: `setColor 31 ${spark_executor_instance}` is set too small, the max could be `setColor 36 ${recommend}`"
else
    echo ">   The max configurable yarn executor instances: `setColor 36 ${recommend}`"
fi
