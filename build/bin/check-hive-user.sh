#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking Hive write permission..."

# test hive or beeline has write permission
HIVE_CLIENT_TYPE=`sh $KYLIN_HOME/bin/get-properties.sh kylin.hive.client`
HIVE_TEST_DB=`sh $KYLIN_HOME/bin/get-properties.sh kylin.job.hive.database.for.intermediatetable`
WORKING_DIR=`sh $KYLIN_HOME/bin/get-properties.sh kylin.hdfs.working.dir`
if [ -z "${HIVE_TEST_DB}" ]
then
    HIVE_TEST_DB=default
fi
RANDNAME=chkenv__${RANDOM}
HIVE_TEST_TABLE=${HIVE_TEST_DB}.${RANDNAME}
HIVE_TEST_TABLE_LOCATION=${WORKING_DIR}/${RANDNAME}

if [ "${HIVE_CLIENT_TYPE}" = "cli" ] 
then
    hive -e "drop table if exists ${HIVE_TEST_TABLE}; create external table ${HIVE_TEST_TABLE} (id INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '$HIVE_TEST_TABLE_LOCATION'; insert into table ${HIVE_TEST_TABLE} values (0); drop table ${HIVE_TEST_TABLE};"
    [[ $? == 0 ]] || quit "ERROR: Hive have no permission to create table in working directory: ${WORKING_DIR}"
    hadoop fs -rm -R -skipTrash ${HIVE_TEST_TABLE_LOCATION}
elif [ ${HIVE_CLIENT_TYPE} = "beeline" ]
then
    HIVE_BEELINE_PARAM=`sh $KYLIN_HOME/bin/get-properties.sh kylin.hive.beeline.params`
    beeline ${HIVE_BEELINE_PARAM} -e "set;" >/dev/null
    [[ $? == 0 ]] || quit "ERROR: Beeline cannot connect with parameter \"${HIVE_BEELINE_PARAM}\". Please configure \"kylin.hive.beeline.params\" in conf/kylin.properties"
    beeline ${HIVE_BEELINE_PARAM} -e "drop table if exists ${HIVE_TEST_TABLE};"
    [[ $? == 0 ]] || quit "ERROR: Beeline have no permission to create/drop table in Hive database '${HIVE_TEST_DB}'"
    beeline ${HIVE_BEELINE_PARAM} -e "create external table ${HIVE_TEST_TABLE} (id INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '$HIVE_TEST_TABLE_LOCATION'; insert overwrite table ${HIVE_TEST_TABLE} values (0); drop table ${HIVE_TEST_TABLE};"
    [[ $? == 0 ]] || quit "ERROR: Beeline have no permission to create table in working directory: ${WORKING_DIR}"
    hadoop fs -rm -R -skipTrash ${HIVE_TEST_TABLE_LOCATION}
else
    quit "ERROR: Only support 'cli' or 'beeline' hive client"
fi
