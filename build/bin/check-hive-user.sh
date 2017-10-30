#!/bin/bash
# Kyligence Inc. License
#title=Checking Hive Usages

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

## ${dir} assigned to $KYLIN_HOME/bin in header.sh
source ${dir}/find-hadoop-conf-dir.sh
source ${dir}/load-hive-conf.sh

echo "Checking Hive write permission..."

# test hive or beeline has write permission

HIVE_CLIENT_TYPE=`$KYLIN_HOME/bin/get-properties.sh kylin.source.hive.client`
HIVE_TEST_DB=`$KYLIN_HOME/bin/get-properties.sh kylin.source.hive.database-for-flat-table`
WORKING_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.env.hdfs-working-dir`
CHECK_TMP_DIR=${WORKING_DIR}/tmp

if [ -z "${HIVE_TEST_DB}" ]
then
    HIVE_TEST_DB=default
fi
RANDNAME=chkenv__${RANDOM}
HIVE_TEST_TABLE=${HIVE_TEST_DB}.${RANDNAME}
HIVE_TEST_TABLE_LOCATION=${WORKING_DIR}/${RANDNAME}

if [ -z "${kylin_hadoop_conf_dir}" ]; then
    hadoop_conf_param=
else
    hadoop_conf_param="--config ${kylin_hadoop_conf_dir}"
fi

if [ "${HIVE_CLIENT_TYPE}" = "cli" ]
then
    hive ${hive_conf_properties} -e "drop table if exists ${HIVE_TEST_TABLE}; create external table ${HIVE_TEST_TABLE} (name STRING,age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '$HIVE_TEST_TABLE_LOCATION'; insert into table ${HIVE_TEST_TABLE} values ('"kylin"',1);"
    [[ $? == 0 ]] || quit "ERROR: Current user has no permission to create Hive table in working directory: ${WORKING_DIR}"

    echo "Checking HCat Available"
    ${dir}/kylin.sh io.kyligence.kap.source.hive.tool.CheckHCatalogJob ${HIVE_TEST_DB} ${RANDNAME} ${CHECK_TMP_DIR}
    [[ $? == 0 ]] || quit "ERROR: Cannot read Hive table (${HIVE_TEST_TABLE}) via HCatInputFormat API. Please check 'logs/check-env.out' for details."
elif [ ${HIVE_CLIENT_TYPE} = "beeline" ]
then
    HQL_TMP_FILE=hql_tmp__${RANDOM}
    HIVE_BEELINE_PARAM=`$KYLIN_HOME/bin/get-properties.sh kylin.source.hive.beeline-params`

    echo "set;" > ${HQL_TMP_FILE}
    beeline ${hive_conf_properties} ${HIVE_BEELINE_PARAM} -f ${HQL_TMP_FILE} >/dev/null
    [[ $? == 0 ]] || { rm -f ${HQL_TMP_FILE}; quit "ERROR: Beeline cannot connect with parameter \"${HIVE_BEELINE_PARAM}\". Please configure \"kylin.source.hive.beeline-params\" in 'conf/kylin.properties'"; }

    echo "drop table if exists ${HIVE_TEST_TABLE};" > ${HQL_TMP_FILE}
    beeline ${hive_conf_properties} ${HIVE_BEELINE_PARAM} -f ${HQL_TMP_FILE}
    [[ $? == 0 ]] || { rm -f ${HQL_TMP_FILE}; quit "ERROR: Current user has no permission to create/drop table in Hive database '${HIVE_TEST_DB}'"; }

    echo "create external table ${HIVE_TEST_TABLE} (name STRING,age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '$HIVE_TEST_TABLE_LOCATION';" > ${HQL_TMP_FILE}
    echo "insert overwrite table ${HIVE_TEST_TABLE} values ('"kylin"',1);" >> ${HQL_TMP_FILE}
    beeline ${hive_conf_properties} ${HIVE_BEELINE_PARAM} -f ${HQL_TMP_FILE}
    [[ $? == 0 ]] || { rm -f ${HQL_TMP_FILE}; quit "ERROR: Current user has no permission to create table in working directory: ${WORKING_DIR}"; }

    echo "Checking HCat Available"
    ${dir}/kylin.sh io.kyligence.kap.source.hive.tool.CheckHCatalogJob ${HIVE_TEST_DB} ${RANDNAME} ${CHECK_TMP_DIR}
    [[ $? == 0 ]] || quit "ERROR: Cannot read Hive table (${HIVE_TEST_TABLE}) via HCatInputFormat API. Please check 'logs/check-env.out' for details."
else
    quit "ERROR: Only support 'cli' or 'beeline' hive client. Please correct 'kylin.source.hive.client' in 'conf/kylin.properties'."
fi

# safeguard cleanup
verbose "Safeguard cleanup..."
if [ "${HIVE_CLIENT_TYPE}" = "cli" ]
then
    hive ${hive_conf_properties} -e "use ${HIVE_TEST_DB}; show tables 'chkenv__*';" | xargs -I '{}' hive ${hive_conf_properties} -e "use ${HIVE_TEST_DB}; drop table {};"
elif [ ${HIVE_CLIENT_TYPE} = "beeline" ]
then
    echo "use ${HIVE_TEST_DB};" > ${HQL_TMP_FILE}
    echo "show tables 'chkenv__*';" >> ${HQL_TMP_FILE}
    beeline ${hive_conf_properties} ${HIVE_BEELINE_PARAM} -f ${HQL_TMP_FILE} | grep "chkenv__[[:digit:]]\+" -o | xargs -I "{}" beeline ${hive_conf_properties} ${HIVE_BEELINE_PARAM} -e "drop table ${HIVE_TEST_DB}.{}"
    rm -f ${HQL_TMP_FILE}
fi

hadoop ${hadoop_conf_param} fs -rm -R -skipTrash "${WORKING_DIR}/chkenv__*"
hadoop ${hadoop_conf_param} fs -rm -R -skipTrash "${CHECK_TMP_DIR}"
exit 0