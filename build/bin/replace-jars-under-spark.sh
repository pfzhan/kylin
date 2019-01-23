#!/bin/bash
# Kyligence Inc. License

source ${KYLIN_HOME}/bin/init-kerberos.sh

initKerberosIfNeeded

BYPASS=${SPARK_HOME}/jars/repalce-jars-bypass

# only replace when has Kerberos
if [[ -z "$(command -v klist)" || -f ${BYPASS} ]]
then
    return
fi
echo "Start replacing hadoop jars under ${SPARK_HOME}/jars."

common_jars=
hdfs_jars=
mr_jars=
yarn_jars=
other_jars=

if [ -n "$FI_ENV_PLATFORM" ]
then
    common_jars=$(find $FI_ENV_PLATFORM/HDFS/hadoop/share/hadoop/common -maxdepth 2 \
    -name "hadoop-annotations-*.jar" -not -name "*test*" \
    -o -name "hadoop-auth-*.jar" -not -name "*test*" \
    -o -name "hadoop-common-*.jar" -not -name "*test*" \
    -o -name "htrace-core-*.jar")

    hdfs_jars=$(find $FI_ENV_PLATFORM/HDFS/hadoop/share/hadoop/hdfs -maxdepth 2 -name "hadoop-hdfs-*" -not -name "*test*" -not -name "*nfs*" -not -name "*datamovement*")

    mr_jars=$(find $FI_ENV_PLATFORM/HDFS/hadoop/share/hadoop/mapreduce -maxdepth 1 \
    -name "hadoop-mapreduce-client-app-*.jar" -not -name "*test*"  \
    -o -name "hadoop-mapreduce-client-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-jobclient-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-shuffle-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-core-*.jar" -not -name "*test*")

    yarn_jars=$(find $FI_ENV_PLATFORM/Yarn/hadoop/share/hadoop/yarn -maxdepth 1 \
    -name "hadoop-yarn-api-*.jar" -not -name "*test*"  \
    -o -name "hadoop-yarn-client-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-web-proxy-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-applicationhistoryservice-*.jar" -not -name "*test*")

    other_jars=$(find $FI_ENV_PLATFORM/HDFS/hadoop/share/hadoop/common/lib/ -maxdepth 1 -name "dynalogger*")

elif [ -d "/opt/cloudera/parcels/CDH" ]
then
    common_jars=$(find /opt/cloudera/parcels/CDH/lib/hadoop -maxdepth 1 \
    -name "hadoop-annotations-*.jar" -not -name "*test*" \
    -o -name "hadoop-auth-*.jar" -not -name "*test*" \
    -o -name "hadoop-common-*.jar" -not -name "*test*")

    hdfs_jars=$(find /opt/cloudera/parcels/CDH/lib/hadoop-hdfs -maxdepth 1 -name "hadoop-hdfs-*" -not -name "*test*" -not -name "*nfs*")

    mr_jars=$(find /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce -maxdepth 1 \
    -name "hadoop-mapreduce-client-app-*.jar" -not -name "*test*"  \
    -o -name "hadoop-mapreduce-client-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-jobclient-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-shuffle-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-core-*.jar" -not -name "*test*")

    yarn_jars=$(find /opt/cloudera/parcels/CDH/lib/hadoop-yarn -maxdepth 1 \
    -name "hadoop-yarn-api-*.jar" -not -name "*test*"  \
    -o -name "hadoop-yarn-client-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-web-proxy-*.jar" -not -name "*test*")

    other_jars=$(find /opt/cloudera/parcels/CDH/jars/ -maxdepth 1 -name "htrace-core4*")
fi

# not consider HDP

jar_list="${common_jars} ${hdfs_jars} ${mr_jars} ${yarn_jars} ${other_jars}"

echo "Find platform specific jars:${jar_list}, will replace with these jars under ${SPARK_HOME}/jars."

find ${SPARK_HOME}/jars -name "htrace-core-*" -exec rm -rf {} \;
find ${SPARK_HOME}/jars -name "hadoop-*2.6.*.jar" -exec rm -rf {} \;

for dir in $jar_list
do
    `cp $dir ${SPARK_HOME}/jars`
done
touch ${BYPASS}
echo "Done hadoop jars replacement under ${SPARK_HOME}/jars."