#!/bin/bash

#three args $key $value $filename
function add_or_reset()
{
        key=$1
        value=$2
        filename=$3
        line=`sed -n '/'"$key"'/=' $filename`
        if [ "$line" == "" ]; then  #Not exist. Append
                echo "${key}=${value}" >> ${filename}
                return
        else #Exist. Replace
                sed -i '/'"$key"'/d' ${filename}
                echo "${key}=${value}" >> ${filename}
        fi
}

#def constant var
spark_executor_cores=4
override_file="${KYLIN_HOME}/conf/kylin.properties"

if [ ! -f ${override_file} ]; then
        echo "${override_file} not exist. Please check"
        exit 1
fi


echo "start setup procedure"
echo "please enter the number of spark vcore. You'd better make sure that it's multiple of ${spark_executor_cores}"
read spark_vcore
expr ${spark_vcore} "+" 10 &> /dev/null
if [ $? -ne 0 ];then
  echo "please enter a number"
  exit 1
fi
spark_executor_instances=`expr ${spark_vcore} / ${spark_executor_cores} `

echo "auto config properties below"
echo "kap.storage.columnar.spark-conf.spark.executor.instances=${spark_executor_instances}"
echo "kap.storage.columnar.spark-conf.spark.executor.cores=${spark_executor_cores}"


add_or_reset kap.storage.columnar.spark-conf.spark.executor.instances ${spark_executor_instances} ${override_file}
add_or_reset kap.storage.columnar.spark-conf.spark.executor.cores ${spark_executor_cores} ${override_file}



