#!/bin/bash
# Kyligence Inc. License


BYPASS=${SPARK_HOME}/jars/replace-jars-bypass

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

cdh_mapreduce_path=$CDH_MR2_HOME

if [[ -z $cdh_mapreduce_path ]]
then
    if [[ -d "/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce" ]]
    then
        cdh_mapreduce_path="/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce"
    else
        cdh_mapreduce_path="/usr/lib/hadoop-mapreduce"
    fi
fi

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

    other_jars1=$(find $FI_ENV_PLATFORM/HDFS/hadoop/share/hadoop/common/lib/ -maxdepth 1 -name "dynalogger*")
    other_jars2=$(find $FI_ENV_PLATFORM/ZooKeeper/zookeeper/ -maxdepth 1 -name "zookeeper-*")
    other_jars="${other_jars1} ${other_jars2}"

    find ${SPARK_HOME}/jars -name "zookeeper-*" -exec rm -rf {} \;

elif [ -d $cdh_mapreduce_path ]
then
    common_jars=$(find $cdh_mapreduce_path/../hadoop -maxdepth 2 \
    -name "hadoop-annotations-*.jar" -not -name "*test*" \
    -o -name "hadoop-auth-*.jar" -not -name "*test*" \
    -o -name "hadoop-common-*.jar" -not -name "*test*")

    hdfs_jars=$(find $cdh_mapreduce_path/../hadoop_hdfs -maxdepth 1 -name "hadoop-hdfs-*" -not -name "*test*" -not -name "*nfs*")

    mr_jars=$(find $cdh_mapreduce_path -maxdepth 1 \
    -name "hadoop-mapreduce-client-app-*.jar" -not -name "*test*"  \
    -o -name "hadoop-mapreduce-client-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-jobclient-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-shuffle-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-core-*.jar" -not -name "*test*")

    yarn_jars=$(find $cdh_mapreduce_path/../hadoop-yarn -maxdepth 1 \
    -name "hadoop-yarn-api-*.jar" -not -name "*test*"  \
    -o -name "hadoop-yarn-client-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-web-proxy-*.jar" -not -name "*test*")


    other_jars=$(find $cdh_mapreduce_path/../../jars -maxdepth 1 -name "htrace-core4*" || find $cdh_mapreduce_path/../hadoop -maxdepth 2 -name "htrace-core4*")
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

# Remove all spaces
jar_list=${jar_list// /}

if [ -z "${jar_list}" ]
then
    echo "Please confirm that the corresponding hadoop jars have been replaced. The automatic replacement program cannot be executed correctly."
else
    touch ${BYPASS}
fi

echo "Done hadoop jars replacement under ${SPARK_HOME}/jars."
