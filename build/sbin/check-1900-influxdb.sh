#!/bin/bash
# Kyligence Inc. License
#title=Checking InfluxDB

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source ${KYLIN_HOME}/sbin/init-kerberos.sh
## init Kerberos if needed
initKerberosIfNeeded

echo "Checking InfluxDB..."

influxdb_address=`${KYLIN_HOME}/bin/get-properties.sh kap.influxdb.address`
influxdb_username=`${KYLIN_HOME}/bin/get-properties.sh kap.influxdb.username`
influxdb_password=`${KYLIN_HOME}/bin/get-properties.sh kap.influxdb.password DEC`
connection=`curl -sL -I "http://${influxdb_address}/ping" | grep "204 No Content"`

[[ -n ${connection} ]] || exit 3

DEFAULT_DATABASE="TEST_DATABASE"
DEFAULT_RETENTION_POLICY="TEST_RETENTION_POLICY"
database_exist=`curl -sG "http://${influxdb_address}/query?pretty=true" --data-urlencode "q=SHOW DATABASES" | grep $DEFAULT_DATABASE`

if [[ -z ${database_exist} ]]; then
    echo "default InfluxDB database does not exist, now create a default database"
    create_database_response=`curl -XPOST "http://${influxdb_address}/query?u=${influxdb_username}&p=${influxdb_password}" --data-urlencode "q=CREATE DATABASE ${DEFAULT_DATABASE}"`
    if [[ ${create_database_response} == *"error"* ]]; then
        echo "cannot create default database, ${create_database_response}"
        exit 3
    fi

    create_retention_policy_response=`curl -XPOST "http://${influxdb_address}/query?u=${influxdb_username}&p=${influxdb_password}" --data-urlencode "q=CREATE RETENTION POLICY ${DEFAULT_RETENTION_POLICY} on ${DEFAULT_DATABASE} duration 30d replication 1 shard duration 7d default"`
    if [[ ${create_retention_policy_response} == *"error"* ]]; then
        echo "cannot create default retention policy, ${create_retention_policy_response}"
        exit 3
    fi
fi

# test write privilege
write_response=`curl -i -XPOST "http://${influxdb_address}/write?u=${influxdb_username}&p=${influxdb_password}&db=${DEFAULT_DATABASE}&rp=${DEFAULT_RETENTION_POLICY}" --data-binary 'mymeas,mytag=1 myfield=90'`
if [[ ${write_response} == *"error"* ]]; then
    echo "write points to InfluxDB failed, ${write_response}"
    exit 3
fi

# drop test measurement
curl -XPOST "http://${influxdb_address}/query?u=${influxdb_username}&p=${influxdb_password}&db=${DEFAULT_DATABASE}" --data-urlencode "q=DROP MEASUREMENT mymeas"

