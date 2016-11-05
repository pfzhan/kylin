#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

WORKING_DIR=`sh $KYLIN_HOME/bin/get-properties.sh kylin.hdfs.working.dir`
TEST_FILE=${WORKING_DIR}/testfile

$(hadoop fs -test -d ${WORKING_DIR}) || quit "ERROR: Please create working directory '${WORKING_DIR}' and grant access permission."

# test if kylin user (current user) has write permission to working directory
touch ./testfile
hadoop fs -put -f ./testfile ${TEST_FILE}
rm -f ./testfile
if [ $? -eq 0 ] 
then
    hadoop fs -rm -skipTrash ${TEST_FILE}
else
    quit "ERROR: Have no permission to create/modify file in working directory: ${WORKING_DIR}"
fi

# test hive or beeline has write permission
HIVE_CLIENT_TYPE=`sh $KYLIN_HOME/bin/get-properties.sh kylin.hive.client`
HIVE_TEST_DB=`sh $KYLIN_HOME/bin/get-properties.sh kylin.job.hive.database.for.intermediatetable`
if [ -z ${HIVE_TEST_DB} ]
then
    HIVE_TEST_DB=default
fi
HIVE_TEST_TABLE=${HIVE_TEST_DB}.test_permission
HIVE_TEST_TABLE_LOCATION=${WORKING_DIR}/test_permission

if [ ${HIVE_CLIENT_TYPE} = "cli" ] 
then
    hive -e "drop table if exists ${HIVE_TEST_TABLE}; create external table ${HIVE_TEST_TABLE} (id INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '$HIVE_TEST_TABLE_LOCATION'; insert into table ${HIVE_TEST_TABLE} values (0); drop table ${HIVE_TEST_TABLE};" || quit "ERROR: Hive have no permission to create table in working directory: ${WORKING_DIR}"
    hadoop fs -rm -R -skipTrash ${HIVE_TEST_TABLE_LOCATION}
elif [ ${HIVE_CLIENT_TYPE} = "beeline" ]
then
    HIVE_BEELINE_PARAM=`sh $KYLIN_HOME/bin/get-properties.sh kylin.hive.beeline.params`
    beeline ${HIVE_BEELINE_PARAM} -e "drop table if exists ${HIVE_TEST_TABLE};" || quit "ERROR: Have no permission to create/drop table in Hive database '${HIVE_TEST_DB}'"
    beeline ${HIVE_BEELINE_PARAM} -e "create external table ${HIVE_TEST_TABLE} (id INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '$HIVE_TEST_TABLE_LOCATION'; insert into table ${HIVE_TEST_TABLE} values (0); drop table ${HIVE_TEST_TABLE};" || quit "ERROR: Beeline have no permission to create table in working directory: ${WORKING_DIR}"
    hadoop fs -rm -R -skipTrash ${HIVE_TEST_TABLE_LOCATION}
else
    quit "ERROR: Only support 'cli' or 'beeline' hive client"
fi
