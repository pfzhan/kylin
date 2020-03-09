#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh $@
source ${KYLIN_HOME}/sbin/init-kerberos.sh
source ${KYLIN_HOME}/sbin/prepare-hadoop-env.sh

## init Kerberos if needed
initKerberosIfNeeded


if [ "$1" == "-v" ]; then
    shift
fi


if [[ $CI_MODE == 'true' ]]
then
    verbose 'in ci mode'
    export KYLIN_HOME=`cd "${KYLIN_HOME}/.."; pwd`
    export CONF_DIR=${KYLIN_HOME}/extensions/examples/test_case_data/sandbox
    export KYLIN_CONF=$CONF_DIR
    export LOG4J_DIR=${KYLIN_HOME}/build/conf
    export SPARK_DIR=${KYLIN_HOME}/build/spark/
    export KYLIN_SPARK_TEST_JAR_PATH=`ls $KYLIN_HOME/src/tool-assembly/target/kap-tool-assembly-*.jar`
    export KAP_HDFS_WORKING_DIR=`$KYLIN_HOME/build/bin/get-properties.sh kylin.env.hdfs-working-dir`
    export KAP_METADATA_URL=`$KYLIN_HOME/build/bin/get-properties.sh kylin.metadata.url`
    export SPARK_ENV_PROPS=`$KYLIN_HOME/build/bin/get-properties.sh kap.storage.columnar.spark-env.`
    export SPARK_CONF_PROPS=`$KYLIN_HOME/build/bin/get-properties.sh kap.storage.columnar.spark-conf.`
    export SPARK_ENGINE_CONF_PROPS=`$KYLIN_HOME/build/bin/get-properties.sh kylin.engine.spark-conf.`
    export SPARK_DRIVER_PORT=`$KYLIN_HOME/build/bin/get-properties.sh kap.storage.columnar.spark-driver-port`
else
    verbose 'in normal mode'
    export KYLIN_HOME=${KYLIN_HOME:-"${dir}/../"}
    export CONF_DIR=${KYLIN_HOME}/conf
    export LOG4J_DIR=${KYLIN_HOME}/conf
    export SPARK_DIR=${KYLIN_HOME}/spark/
    export KYLIN_SPARK_TEST_JAR_PATH=`ls $KYLIN_HOME/tool/kap-tool-*.jar`
    export KAP_HDFS_WORKING_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.env.hdfs-working-dir`
    export KAP_METADATA_URL=`$KYLIN_HOME/bin/get-properties.sh kylin.metadata.url`
    export SPARK_ENV_PROPS=`$KYLIN_HOME/bin/get-properties.sh kap.storage.columnar.spark-env.`
    export SPARK_CONF_PROPS=`$KYLIN_HOME/bin/get-properties.sh kap.storage.columnar.spark-conf.`
    export SPARK_ENGINE_CONF_PROPS=`$KYLIN_HOME/bin/get-properties.sh kylin.engine.spark-conf.`
    export SPARK_DRIVER_PORT=`$KYLIN_HOME/bin/get-properties.sh kap.storage.columnar.spark-driver-port`

    if [ ! -f ${KYLIN_HOME}/commit_SHA1 ]
    then
        quit "Seems you're not in binary package, did you forget to set CI_MODE=true?"
    fi
fi

source ${KYLIN_HOME}/sbin/prepare-hadoop-conf-dir.sh
export KAP_SPARK_IDENTIFIER=$RANDOM
#export KAP_HDFS_APPENDER_JAR=`basename ${KYLIN_SPARK_JAR_PATH}`

# get local ip for htrace-zipkin use
if [ -z "$ZIPKIN_HOSTNAME" ]
then
    export ZIPKIN_HOSTNAME=`hostname`
fi
echo "ZIPKIN_HOSTNAME is set to ${ZIPKIN_HOSTNAME}"
echo "ZIPKIN_SCRIBE_PORT is set to ${ZIPKIN_SCRIBE_PORT}"

verbose "KYLIN_HOME is set to ${KYLIN_HOME}"
verbose "CONF_DIR is set to ${CONF_DIR}"
verbose "SPARK_DIR is set to ${SPARK_DIR}"
#verbose "KYLIN_SPARK_JAR_PATH is set to ${KYLIN_SPARK_JAR_PATH}"

mkdir -p ${KYLIN_HOME}/logs

#auto detect SPARK_HOME
source ${KYLIN_HOME}/sbin/do-check-and-prepare-spark.sh
if [ -z "$SPARK_HOME" ]
then
    if [ -d ${SPARK_DIR} ]
    then
        export SPARK_HOME=${SPARK_DIR}
    else
        quit 'Please make sure SPARK_HOME has been set (export as environment variable first)'
    fi
fi
echo "SPARK_HOME is set to ${SPARK_HOME}"

function config_item_java_options_add_suffix()
{
    source_conf_tmp=$1
    conf_key_tmp=$2
    conf_value_tmp=$3

    if [[ "${source_conf_tmp}" == *${conf_key_tmp}=* ]]; then
        echo "${source_conf_tmp}" | sed "s~${conf_key_tmp}=~${conf_key_tmp}=${conf_value_tmp} ~g"
    else
        echo "${source_conf_tmp} --conf ${conf_key_tmp}=${conf_value_tmp} "
    fi
}

function config_item_yarn_dist_add_suffix()
{
    source_conf_tmp=$1
    conf_key_tmp=$2
    conf_value_tmp=$3

    if [[ "${source_conf_tmp}" == *${conf_key_tmp}=* ]]; then
        echo "${source_conf_tmp}" | sed "s~${conf_key_tmp}=~${conf_key_tmp}=${conf_value_tmp},~g"
    else
        echo "${source_conf_tmp} --conf ${conf_key_tmp}=${conf_value_tmp} "
    fi
}

function retrieveSparkEnvProps()
{
 # spark envs
    for kv in `echo "$SPARK_ENV_PROPS"`
    do
        key=`echo "$kv" |  awk '{ n = index($0,"="); print substr($0,0,n-1)}'`
        existingValue=`printenv ${key}`
        if [ -z "$existingValue" ]
        then
            verbose "export" `eval "verbose $kv"`
            eval "export $kv"
        else
            verbose "$key already has value: $existingValue, use it"
        fi
    done

    # spark conf
    confStr=`echo "$SPARK_CONF_PROPS" |  awk '{ print "--conf " "\"" $0 "\""}' | tr '\n' ' ' `
    KAP_KERBEROS_ENABLED=`$KYLIN_HOME/bin/get-properties.sh kap.kerberos.enabled`
    if [[ "${KAP_KERBEROS_ENABLED}" == "true" ]]
    then
        confStr=`echo ${confStr} --conf 'spark.hadoop.hive.metastore.sasl.enabled=true'`
        confStr=$(config_item_java_options_add_suffix "${confStr}" "spark.yarn.am.extraJavaOptions" "-Djava.security.krb5.conf=krb5.conf")
        confStr=$(config_item_java_options_add_suffix "${confStr}" "spark.executor.extraJavaOptions" "-Djava.security.krb5.conf=krb5.conf")
        confStr=$(config_item_java_options_add_suffix "${confStr}" "spark.driver.extraJavaOptions" "-Djava.security.krb5.conf=${KYLIN_HOME}/conf/krb5.conf")

        code_tmp=`echo "$SPARK_CONF_PROPS" | grep -c -E "spark.yarn.dist.files=.*krb5.conf"`
        if [[ ${code_tmp} == 0 ]];then
            confStr=$(config_item_yarn_dist_add_suffix "${confStr}" "spark.yarn.dist.files" "${KYLIN_HOME}/conf/krb5.conf")
        fi
    fi

    engineConfStr=`echo "$SPARK_ENGINE_CONF_PROPS" |  awk '{ print "--conf " "\"" $0 "\""}' | tr '\n' ' ' `
    if [[ "${KAP_KERBEROS_ENABLED}" == "true" ]]
    then
        engineConfStr=`echo ${engineConfStr} --conf 'spark.hadoop.hive.metastore.sasl.enabled=true'`
        engineConfStr=$(config_item_java_options_add_suffix "${engineConfStr}" "spark.yarn.am.extraJavaOptions" "-Djava.security.krb5.conf=krb5.conf")
        engineConfStr=$(config_item_java_options_add_suffix "${engineConfStr}" "spark.executor.extraJavaOptions" "-Djava.security.krb5.conf=krb5.conf")
        engineConfStr=$(config_item_java_options_add_suffix "${engineConfStr}" "spark.driver.extraJavaOptions" "-Djava.security.krb5.conf=${KYLIN_HOME}/conf/krb5.conf")

        code_tmp=`echo "$SPARK_ENGINE_CONF_PROPS" | grep -c -E "spark.yarn.dist.files=.*krb5.conf"`
        if [[ ${code_tmp} == 0 ]];then
            engineConfStr=$(config_item_yarn_dist_add_suffix "${engineConfStr}" "spark.yarn.dist.files" "${KYLIN_HOME}/conf/krb5.conf")
        fi
    fi

    confStr=`removeInvalidSparkConfValue "$SPARK_CONF_PROPS" "$confStr"`
    engineConfStr=`removeInvalidSparkConfValue "$SPARK_ENGINE_CONF_PROPS" "$engineConfStr"`

    verbose "additional confs spark-submit: $confStr"
    verbose "additional confs spark-sql: $engineConfStr"
}

function removeInvalidSparkConfValue() {
    SAVEIFS=$IFS
    IFS=$'\n'
    sparkConfArray=($1)
    result=$2

    for (( i=0; i<${#sparkConfArray[@]}; i++ ))
    do
        conf=${sparkConfArray[$i]}
        confValuesString=${conf#*=}
        IFS=' ' read -r -a confValues <<< "$confValuesString"
        for (( j=0; j<${#confValues[@]}; j++ ))
        do
            confValue=${confValues[$j]}
            if [[ $confValue == *"\${"* ]]; then
                result=`echo ${result//${confValue}/}`
            fi
        done
    done

    IFS=$SAVEIFS

    echo "$result"
}

if [ "$1" == "test" ]
then
    source ${KYLIN_HOME}/sbin/find-working-dir.sh
    echo "Starting test spark with conf"

    retrieveSparkEnvProps
    echo "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"

    local_input_dir=${KYLIN_HOME}/logs/tmp
    input_file=spark_client_test_input
    full_input_file=${local_input_dir}/${input_file}
    mkdir -p ${local_input_dir}

    [[ ! -f ${full_input_file} ]] || rm -f ${full_input_file}
    echo "Hello Spark Client" >> ${full_input_file};

    hadoop ${KAP_HADOOP_PARAM} fs -put -f ${full_input_file} ${KAP_WORKING_DIR}

    spark_submit='$SPARK_HOME/bin/spark-submit '
    spark_submit_conf=' --class io.kyligence.kap.tool.setup.KapSparkTaskTestCLI --name Test  $KYLIN_SPARK_TEST_JAR_PATH ${KAP_WORKING_DIR}/${input_file} '
    submitCommand=${spark_submit}${confStr}${spark_submit_conf}
    verbose "The submit command is: $submitCommand"
    eval $submitCommand
    if [ $? == 0 ];then
        hadoop ${KAP_HADOOP_PARAM} fs -rm -r -skipTrash ${KAP_WORKING_DIR}/${input_file}
        rm -rf ${full_input_file}
    else
        hadoop ${KAP_HADOOP_PARAM} fs -rm -r -skipTrash ${KAP_WORKING_DIR}/${input_file}
        rm -rf ${full_input_file}
        quit "ERROR: Test of submitting spark job failed,error when testing spark with spark configurations in Kyligence Enterprise!"
    fi

    echo "===================================="
    echo "Testing spark-sql..."
    if [[ $(hadoop version) != *"mapr"* ]]; then
        if [ ! -f $kylin_hadoop_conf_dir/hive-site.xml ]; then
            quit "ERROR:Test of spark-sql failed,$kylin_hadoop_conf_dir is not valid hadoop dir conf because hive-site.xml is missing!"
        fi
    fi
    HIVE_TEST_DB=`$KYLIN_HOME/bin/get-properties.sh kylin.source.hive.database-for-flat-table`
    if [[ -z "${HIVE_TEST_DB}" ]]; then
        HIVE_TEST_DB=default
    fi
    CHECK_TMP_DIR=${WORKING_DIR}/tmp
    CHECK_TABLE_NAME="kylin_table_for_sparktest"
    HIVE_TEST_TABLE=${HIVE_TEST_DB}.${CHECK_TABLE_NAME}
    HIVE_TEST_TABLE_LOCATION=${KAP_HDFS_WORKING_DIR}/"_check_env_tmp"/${CHECK_TABLE_NAME}
    SPARK_HQL_TMP_FILE=spark_hql_tmp__${RANDOM}
    spark_sql="${SPARK_HOME}/bin/spark-sql"
    spark_sql_command="export HADOOP_CONF_DIR=${kylin_hadoop_conf_dir} && ${spark_sql} ${engineConfStr} -f ${SPARK_HQL_TMP_FILE}"
    echo "use ${HIVE_TEST_DB};" > ${SPARK_HQL_TMP_FILE}
    eval $spark_sql_command
    [[ $? == 0 ]] || { rm -f ${SPARK_HQL_TMP_FILE}; quit "ERROR: Test of spark-sql failed"; }

    echo "drop table if exists ${HIVE_TEST_TABLE};" > ${SPARK_HQL_TMP_FILE}
    eval $spark_sql_command
    [[ $? == 0 ]] || { rm -f ${SPARK_HQL_TMP_FILE}; quit "ERROR: Current user has no permission to create/drop table in Hive database '${HIVE_TEST_DB}'"; }

    echo "create table ${HIVE_TEST_TABLE} (name STRING,age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '${HIVE_TEST_TABLE_LOCATION}';" > ${SPARK_HQL_TMP_FILE}
    eval $spark_sql_command
    [[ $? == 0 ]] || { rm -f ${SPARK_HQL_TMP_FILE}; quit "ERROR: Current user has no permission to create table in working directory: ${WORKING_DIR}"; }

    echo "drop table if exists ${HIVE_TEST_TABLE}2;" > ${SPARK_HQL_TMP_FILE}
    eval $spark_sql_command
    [[ $? == 0 ]] || { rm -f ${SPARK_HQL_TMP_FILE}; quit "ERROR: Current user has no permission to create/drop table in Hive database '${HIVE_TEST_DB}'"; }

    echo "kylin,1" | hadoop fs -put - ${HIVE_TEST_TABLE_LOCATION}/data.txt
    echo "create table ${HIVE_TEST_TABLE}2 (name STRING,age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '${HIVE_TEST_TABLE_LOCATION}2';" > ${SPARK_HQL_TMP_FILE}
    if [[ $(isHDP_3_1) == 0 ]]; then
        echo "insert overwrite table ${HIVE_TEST_TABLE}2 select * from ${HIVE_TEST_TABLE};" >> ${SPARK_HQL_TMP_FILE}
    fi
    eval $spark_sql_command
    [[ $? == 0 ]] || { rm -f ${SPARK_HQL_TMP_FILE}; quit "ERROR: Current user has no permission to write table in working directory: ${WORKING_DIR}"; }

    # safeguard cleanup
    verbose "Safeguard cleanup..."

    #drop test table
    echo "drop table if exists ${HIVE_TEST_TABLE}" > ${SPARK_HQL_TMP_FILE}
    eval $spark_sql_command

    echo "drop table if exists ${HIVE_TEST_TABLE}2" > ${SPARK_HQL_TMP_FILE}
    eval $spark_sql_command

    rm -f ${SPARK_HQL_TMP_FILE}
    hadoop fs -rm -R -skipTrash "${WORKING_DIR}/_check_env_tmp/"
    exit 0
else
    quit "usage: spark-test.sh test"
fi
