#!/bin/bash
# Kyligence Inc. License
#title=Checking InfluxDB

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source ${dir}/init-kerberos.sh
## init Kerberos if needed
initKerberosIfNeeded

echo "Checking InfluxDB..."

influxdb_address=`${dir}/get-properties.sh kap.influxdb.address`
influxdb_username=`${dir}/get-properties.sh kap.influxdb.username`
influxdb_password=`${dir}/get-properties.sh kap.influxdb.password`
connection=`curl -sL -I "http://${influxdb_address}/ping" | grep "204 No Content"`

[[ -n ${connection} ]] || quit "ERROR: InfluxDB service is not up, please make sure InfluxDB service is up"

DEFAULT_DATABASE="KE_HISTORY"
DEFAULT_RETENTION_POLICY="KE_HISTORY_RP"
database_exist=`curl -sG "http://${influxdb_address}/query?pretty=true" --data-urlencode "q=SHOW DATABASES" | grep $DEFAULT_DATABASE`

if [[ -z ${database_exist} ]]; then
    echo "default InfluxDB database does not exist, now create a default database"
    create_database_response=`curl -XPOST "http://${influxdb_address}/query?u=${influxdb_username}&p=${influxdb_password}" --data-urlencode "q=CREATE DATABASE ${DEFAULT_DATABASE}"`
    if [[ ${create_database_response} == *"error"* ]]; then
        quit "cannot create default database, ${create_database_response}"
    fi

    create_retention_policy_response=`curl -XPOST "http://${influxdb_address}/query?u=${influxdb_username}&p=${influxdb_password}" --data-urlencode "q=CREATE RETENTION POLICY ${DEFAULT_RETENTION_POLICY} on ${DEFAULT_DATABASE} duration 30d replication 1 shard duration 7d default"`
    if [[ ${create_retention_policy_response} == *"error"* ]]; then
        quit "cannot create default retention policy, ${create_retention_policy_response}"
    fi
fi

# test write privilege
write_response=`curl -i -XPOST "http://${influxdb_address}/write?u=${influxdb_username}&p=${influxdb_password}&db=${DEFAULT_DATABASE}&rp=${DEFAULT_RETENTION_POLICY}" --data-binary 'mymeas,mytag=1 myfield=90'`
if [[ ${write_response} == *"error"* ]]; then
    quit "write points to InfluxDB failed, ${write_response}"
fi

# drop test measurement
curl -XPOST "http://${influxdb_address}/query?u=${influxdb_username}&p=${influxdb_password}&db=${DEFAULT_DATABASE}" --data-urlencode "q=DROP MEASUREMENT mymeas"

