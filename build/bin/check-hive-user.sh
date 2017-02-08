#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking Hive write permission..."

# test hive or beeline has write permission
HIVE_CLIENT_TYPE=`bash $KYLIN_HOME/bin/get-properties.sh kylin.source.hive.client`
HIVE_TEST_DB=`bash $KYLIN_HOME/bin/get-properties.sh kylin.source.hive.database-for-flat-table`
WORKING_DIR=`bash $KYLIN_HOME/bin/get-properties.sh kylin.env.hdfs-working-dir`
if [ -z "${HIVE_TEST_DB}" ]
then
    HIVE_TEST_DB=default
fi
RANDNAME=chkenv__${RANDOM}
HIVE_TEST_TABLE=${HIVE_TEST_DB}.${RANDNAME}
HIVE_TEST_TABLE_LOCATION=${WORKING_DIR}/${RANDNAME}

if [ "${HIVE_CLIENT_TYPE}" = "cli" ] 
then
    hive -e "drop table if exists ${HIVE_TEST_TABLE}; create external table ${HIVE_TEST_TABLE} (name STRING,age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '$HIVE_TEST_TABLE_LOCATION'; insert into table ${HIVE_TEST_TABLE} values ('"kylin"',1);"
    [[ $? == 0 ]] || quit "ERROR: Hive have no permission to create table in working directory: ${WORKING_DIR}"
    echo "Checking HCat Available"
    export ENABLE_CHECK_ENV=false
    ret=`./kylin.sh io.kyligence.kap.source.hive.tool.CheckHCatalogJob ${HIVE_TEST_DB} ${RANDNAME} /tmp/kylin/check_hcatalog`
    [[ $? == 0 ]] || quit "ERROR: Can not get Hive table data via HCatInputFormat"
    hive -e "drop table ${HIVE_TEST_TABLE};"
    hadoop fs -rm -R -skipTrash ${HIVE_TEST_TABLE_LOCATION}
elif [ ${HIVE_CLIENT_TYPE} = "beeline" ]
then
    HQL_TMP_FILE=hql_tmp__${RANDOM}
    HIVE_BEELINE_PARAM=`bash $KYLIN_HOME/bin/get-properties.sh kylin.source.hive.beeline-params`

    echo "set;" > ${HQL_TMP_FILE}
    beeline ${HIVE_BEELINE_PARAM} -f ${HQL_TMP_FILE} >/dev/null
    [[ $? == 0 ]] || { rm -f ${HQL_TMP_FILE}; quit "ERROR: Beeline cannot connect with parameter \"${HIVE_BEELINE_PARAM}\". Please configure \"kylin.source.hive.beeline-params\" in conf/kylin.properties"; }

    echo "drop table if exists ${HIVE_TEST_TABLE};" > ${HQL_TMP_FILE}
    beeline ${HIVE_BEELINE_PARAM} -f ${HQL_TMP_FILE}
    [[ $? == 0 ]] || { rm -f ${HQL_TMP_FILE}; quit "ERROR: Beeline have no permission to create/drop table in Hive database '${HIVE_TEST_DB}'"; }

    echo "create external table ${HIVE_TEST_TABLE} (name STRING,age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '$HIVE_TEST_TABLE_LOCATION';" > ${HQL_TMP_FILE}
    echo "insert overwrite table ${HIVE_TEST_TABLE} values ('"kylin"',1);" >> ${HQL_TMP_FILE}
    beeline ${HIVE_BEELINE_PARAM} -f ${HQL_TMP_FILE}
    [[ $? == 0 ]] || { rm -f ${HQL_TMP_FILE}; quit "ERROR: Beeline have no permission to create table in working directory: ${WORKING_DIR}"; }

    echo "Checking HCat Available"
    export ENABLE_CHECK_ENV=false
    ret=`./kylin.sh io.kyligence.kap.source.hive.tool.CheckHCatalogJob ${HIVE_TEST_DB} ${RANDNAME} /tmp/kylin/check_hcatalog`
    [[ $? == 0 ]] || quit "ERROR: Can not get Hive table data via HCatInputFormat"
    beeline ${HIVE_BEELINE_PARAM} -e "drop table ${HIVE_TEST_TABLE};"
    rm -f ${HQL_TMP_FILE}
    hadoop fs -rm -R -skipTrash ${HIVE_TEST_TABLE_LOCATION}
else
    quit "ERROR: Only support 'cli' or 'beeline' hive client"
fi

# safeguard cleanup
verbose "Safeguard Cleanup..."
hive -e "use ${HIVE_TEST_DB}; show tables 'chkenv__*';" | xargs -I '{}' hive -e "use ${HIVE_TEST_DB}; drop table {};"
hadoop fs -rm -R -skipTrash "${WORKING_DIR}/chkenv__*"
exit 0
