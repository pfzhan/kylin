#!/usr/bin/env bash

function getHadoopDistribution() {
    hadoop_version=`hadoop version`
    if [[ $hadoop_version == *"cdh"* ]]; then
      echo "We are in the CDH!"
      hadoop_distribution="cdh"
    elif [[ $hadoop_version == *"hdp"* ]]; then
      echo "We are in the HDP!"
      hadoop_distribution="hdp"
    elif [[ $hadoop_version == *"fi"* ]] || [[ $hadoop_version == *"rw11"* ]]; then
      echo "We are in the FushionInsight!"
      hadoop_distribution="fi"
    elif [[ $hadoop_version == *"mapr"* ]]; then
      echo "We are in the MapR!"
      hadoop_distribution="mapr"
    else
      echo "Unknown env!"
      hadoop_distribution="unknown"
    fi
}

function runTest() {
    export root_dir="${dir}/build/smoke_test/"
    source ${root_dir}/venv/bin/activate
    echo "pip3 is" `which pip3`
    echo "start to run compatibility test on ${hadoop_distribution}"
    export PYTHONPATH=${dir}/build/smoke_test:${PYTHONPATH}
    if [[ $PYTEST_MARK == *"ALL"* ]]; then
        pytest -m "p1" --alluredir ${allure_report} ${root_dir} --junitxml=report_p1.xml
        pytest -m "smoketest" --alluredir ${allure_report} ${root_dir} --junitxml=report_smoketest.xml
        pytest -m "kitest" --alluredir ${allure_report} ${root_dir} --junitxml=report_kitest.xml
        pytest -m "view_sampling" --alluredir ${allure_report} ${root_dir} --junitxml=report_view_sampling.xml
    else
        echo "Run single test case."
        pytest -m ${PYTEST_MARK} --alluredir ${allure_report} ${root_dir}
    fi
    echo "test done"
}

function runApiPermissionCheck() {
    export PYTHONIOENCODING=utf-8
    echo "start run api permission check"
    apiCheckPath=${dir}/build/smoke_test/api_permission_check/compare_result.py
    python ${apiCheckPath}
    echo "end api permission check"
}

getHadoopDistribution

metadataName=smoke_newten_${hadoop_distribution}

ZK_STR=${1:-localhost:2181}
INFLUXDB_ADDRESS=${2:-10.1.2.172:8086}
INFLUXDB_RPC_ADDRESS=${3:-10.1.2.172:8083}
METASTORE=${4:-postgresql}
PYTEST_MARK=${5:-ALL}
METADATA_NAME=${6:-$metadataName}
PG_HOST=${7:-sandbox}
PG_PORT=${8:-5432}
METADATA_TABLE=${METADATA_NAME}
AUDITLOG_TABLE=${METADATA_NAME}_audit_log
SESSION_TABLE=${METADATA_NAME}_session
SESSION_ATTRIBUTES_TABLE=${METADATA_NAME}_session_attributes

echo "The metadata name is ${METADATA_NAME}"

echo "InfluxDB address is ${INFLUXDB_ADDRESS}"
dir=`pwd`
echo "smoke test dir is $dir"

kill -9 $(lsof -t -i:17071)

rm -rf ${dir}/dist/Kyligence-Enterprise-*/

PKG_PATH=${dir}/dist/Kyligence-Enterprise-*.tar.gz
tar -zxf ${PKG_PATH} -C ${dir}/dist/
cd ${dir}/dist/Kyligence-Enterprise-*/
export KYLIN_HOME=`pwd`
cd -
echo $KYLIN_HOME

#prepare python virtualenv
echo "prepare python virtualenv"
bash $dir/build/smoke_test/bin/env/prepare-python-env.sh

cd $KYLIN_HOME/conf/

sed -i "\$a kylin.env.zookeeper-connect-string=${ZK_STR}" kylin.properties
sed -i "\$a kylin.influxdb.address=${INFLUXDB_ADDRESS}" kylin.properties
sed -i "\$a kylin.metrics.influx-rpc-service-bind-address=${INFLUXDB_RPC_ADDRESS}" kylin.properties
sed -i "\$a server.port=17071" kylin.properties
sed -i "\$a kylin.engine.spark-conf.spark.executor.instances=2" kylin.properties
sed -i "\$a kylin.storage.columnar.spark-conf.spark.executor.cores=2" kylin.properties
sed -i "\$a kylin.storage.columnar.spark-conf.spark.driver.memory=1024m" kylin.properties
sed -i "\$a kylin.storage.columnar.spark-conf.spark.executor.memory=1024m" kylin.properties
sed -i "\$a kylin.storage.columnar.spark-conf.spark.executor.instances=1" kylin.properties
sed -i "\$a kylin.storage.columnar.spark-conf.spark.yarn.executor.memoryOverhead=512" kylin.properties
if [[ $hadoop_distribution == "fi" ]]; then
    cp -f /root/user.keytab $KYLIN_HOME/conf
    cp -f /root/krb5.conf $KYLIN_HOME/conf
    sed -i "\$a kylin.kerberos.platform=FI" kylin.properties
    sed -i "\$a kylin.kerberos.principal=newten" kylin.properties
    sed -i "\$a kylin.kerberos.keytab=user.keytab" kylin.properties
    sed -i "\$a kylin.kerberos.enabled=true" kylin.properties
    sed -i "\$a kylin.kerberos.krb5-conf=krb5.conf" kylin.properties
    sed -i "\$a kylin.kerberos.cache=newten_cache" kylin.properties
    sed -i "\$a kylin.kerberos.zookeeper-server-principal=zookeeper/hadoop.hadoop.com" kylin.properties
    sed -i "\$a kylin.engine.spark-conf.spark.yarn.queue=smoke" kylin.properties
    sed -i "\$a kylin.storage.columnar.spark-conf.spark.yarn.queue=smoke" kylin.properties
    sed -i "\$a kylin.engine.spark-conf.spark.yarn.principal=newten" kylin.properties
    sed -i "\$a kylin.engine.spark-conf.spark.yarn.keytab=/etc/user.keytab" kylin.properties
    sed -i "\$a kylin.engine.spark-conf.spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/etc/jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf -Dzookeeper.server.principal=zookeeper/hadoop.hadoop.com -Dhdp.version=current -Dlog4j.configuration=spark-executor-log4j.properties -Dlog4j.debug -Dkylin.hdfs.working.dir=\${kylin.env.hdfs-working-dir} -Dkap.metadata.identifier=\${kylin.metadata.url.identifier} -Dkap.spark.category=job -Dkap.spark.project=\${job.project} -Dkap.spark.identifier=\${job.id} -Dkap.spark.jobName=\${job.stepId} -Duser.timezone=\${user.timezone}" kylin.properties
    sed -i "\$a kylin.engine.spark-conf.spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/etc/jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf" kylin.properties
else
    sed -i "\$a kylin.engine.spark-conf.spark.yarn.queue=default" kylin.properties
fi


sed -i "\$a kylin.metadata.url=${METADATA_NAME}@jdbc,driverClassName=org.postgresql.Driver,url=jdbc:postgresql://${PG_HOST}:${PG_PORT}/kylin,username=postgres,password=kylin" kylin.properties
export PGPASSWORD=kylin
export PSQL="/usr/pgsql-10/bin/psql"
export DROP_TRABLE="${PSQL} -h ${PG_HOST} -p ${PG_PORT} -U postgres -d kylin -c \"drop table if exists"
export METADATA_ERASE_CMD="${DROP_TRABLE} ${METADATA_TABLE}\""
export METADATA_ERASE_CMD=${METADATA_ERASE_CMD}" && ${DROP_TRABLE} ${AUDITLOG_TABLE}\""
export METADATA_ERASE_CMD=${METADATA_ERASE_CMD}" && ${DROP_TRABLE} ${SESSION_TABLE}\""
export METADATA_ERASE_CMD=${METADATA_ERASE_CMD}" && ${DROP_TRABLE} ${SESSION_ATTRIBUTES_TABLE}\""

# clean metadata in metastore
echo "Start erase metadata"
eval $METADATA_ERASE_CMD
if [[ $? -ne 0 ]]; then
    echo "Failed to erase metadata"
fi

# clean data on HDFS
hdfs dfs -rm -r /kylin/${METADATA_NAME}

# skip check env
# touch $KYLIN_HOME/bin/check-env-bypass

# start KE 4.x
#touch $KYLIN_HOME/bin/check-env-bypass
bash $KYLIN_HOME/bin/kylin.sh start
echo "Wait 2 minutes for starting KE 4.x service"
sleep 2m

# return kyQA-Quard dir
cd -

# run pytest
export PYTHON_VENV_HOME=$dir/build/smoke_test/venv
allure_report=$dir/Report
runTest

runApiPermissionCheck

# unset postgres password
unset PGPASSWORD

# stop KE 4
echo "stop KE 4..."
bash $KYLIN_HOME/bin/kylin.sh stop
