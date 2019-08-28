#!/bin/bash
# Kyligence Inc. License

if [[ -z $KYLIN_HOME ]];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

cd $KYLIN_HOME/grafana

PORT=$(grep -w "http_port =" conf/defaults.ini |tr -d '[:space:]' | cut -d'=' -f2)
echo "Grafana port is ${PORT}"

PID=`netstat -tpln 2>/dev/null | grep "\<$PORT\>" | awk '{print $7}' | sed "s/\// /g" | awk '{print $1}'`
if [[ ! -z "${PID}" ]];then
    echo "Grafana is running, stop it first, PID is ${PID}"
    exit 0
fi

metadata_url_prefix=`$KYLIN_HOME/bin/get-properties.sh kylin.metadata.url`
## check whether it contain '@' mark,if it exists, extract the content before it
mark=`echo ${metadata_url_prefix} | grep "@"`
if [ ${#mark} -ne 0 ]
then
    metadata_url_prefix=`echo ${metadata_url_prefix} | awk -F'@' '{print $1}'`
fi
metrics_db_suffix=`$KYLIN_HOME/bin/get-properties.sh kap.metrics.influx.db`

export KE_METRICS_DATABASE=${metadata_url_prefix}_${metrics_db_suffix}
export INFLUXDB_ADDRESS=`$KYLIN_HOME/bin/get-properties.sh kap.influxdb.address`
export INFLUXDB_USERNAME=`$KYLIN_HOME/bin/get-properties.sh kap.influxdb.username`
export INFLUXDB_PASSWORD=`$KYLIN_HOME/bin/get-properties.sh kap.influxdb.password`

echo "Influxdb Address: $INFLUXDB_ADDRESS"
echo "Metrics Database: $KE_METRICS_DATABASE"

if [[ -f "${KYLIN_HOME}/conf/grafana.ini" ]]; then
    nohup bin/grafana-server --config ${KYLIN_HOME}/conf/grafana.ini web > /dev/null 2>&1 &
else
    nohup bin/grafana-server web > /dev/null 2>&1 &
fi


echo "Grafana starting..."

try_times=30
while [[ ${try_times} -gt 0 ]];do
    sleep 3
    PID=`netstat -tpln 2>/dev/null | grep "\<$PORT\>" | awk '{print $7}' | sed "s/\// /g" | awk '{print $1}'`
    if [[ ! -z "${PID}" ]];then
        break
    fi
    let try_times-=1
done

if [[ ${try_times} -le 0 ]];then
    echo "Grafana start timeout."
    exit 0
fi

echo "Grafana started, PID is $PID"