#!/bin/bash
# Kyligence Inc. License

WORKING_DIR=`sh $KYLIN_HOME/bin/get-properties.sh kylin.hdfs.working.dir`
TEST_FILE=${WORKING_DIR}/testfile

function quit {
    echo Error: "$@"
    rm ./testfile
    exit 1
}

$(hadoop fs -test -d ${WORKING_DIR}) || quit "Please create working directory"

# test if kylin user (current user) has write permission to working directory
touch ./testfile
hadoop fs -put -f ./testfile ${TEST_FILE}
if [ $? -eq 0 ] 
then
    hadoop fs -rm ${TEST_FILE}
else
    quit "user '`whoami`' have no permission to create/modify file in working directory: ${WORKING_DIR}"
fi

# test hive or beeline has write permission
HIVE_CLIENT_TYPE=`sh $KYLIN_HOME/bin/get-properties.sh kylin.hive.client`
HIVE_TEST_TABLE=test_permission
HIVE_TEST_TABLE_LOCATION=${WORKING_DIR}/test_permission
if (( ${HIVE_CLIENT_TYPE} == "cli" ))
then
    hive -e "drop table if exists ${HIVE_TEST_TABLE}; create external table ${HIVE_TEST_TABLE} (id INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '$HIVE_TEST_TABLE_LOCATION'; insert into table ${HIVE_TEST_TABLE} values (0); drop table ${HIVE_TEST_TABLE};" || quit "hive have no permission to create table in working directory: ${WORKING_DIR}"
elif (( ${HIVE_CLIENT_TYPE} == "beeline" ))
then
    HIVE_BEELINE_PARAM=`sh $KYLIN_HOME/bin/get-properties.sh kylin.hive.beeline.params`
    beeline ${HIVE_BEELINE_PARAM} -e "drop table if exists ${HIVE_TEST_TABLE}; create external table ${HIVE_TEST_TABLE} (id INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '$HIVE_TEST_TABLE_LOCATION'; insert into table ${HIVE_TEST_TABLE} values (0); drop table ${HIVE_TEST_TABLE};" || quit "hive have no permission to create table in working directory: ${WORKING_DIR}" || quit "beeline have no permission to create table in working directory: ${WORKING_DIR}"
else
    quit "Only support 'cli' or 'beeline' hive client"
fi

rm ./testfile
