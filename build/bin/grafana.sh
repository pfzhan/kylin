#!/bin/bash
# Kyligence Inc. License

if [[ -z $KYLIN_HOME ]];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

cd $KYLIN_HOME/grafana

GRAFANA_PORT=$(grep -w "http_port =" conf/defaults.ini |tr -d '[:space:]' | cut -d'=' -f2)
echo "Grafana port is $GRAFANA_PORT"

GRAFANA_PID=$(lsof -t -i:$GRAFANA_PORT)
if [[ $GRAFANA_PID ]];then
    echo "Grafana is running, stop it first, PID is $GRAFANA_PID"
    exit 0
fi

export INFLUXDB_ADDRESS=`$KYLIN_HOME/bin/get-properties.sh kap.influxdb.address`
export INFLUXDB_USERNAME=`$KYLIN_HOME/bin/get-properties.sh kap.influxdb.username`
export INFLUXDB_PASSWORD=`$KYLIN_HOME/bin/get-properties.sh kap.influxdb.password`

echo "influxdb address: $INFLUXDB_ADDRESS"

nohup bin/grafana-server web &

echo "Grafana starting..."

try_times=30
while [ $try_times -gt 0 ];do
    if lsof -t -i:$GRAFANA_PORT > /dev/null;then
        break
    else
        let try_times-=1
        sleep 5
    fi
done

if [ $try_times -le 0 ];then
    echo "Grafana start timeout."
    exit 0
fi

echo "Grafana started, PID is $(lsof -t -i:$GRAFANA_PORT)"