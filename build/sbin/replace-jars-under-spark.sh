#!/bin/bash

##
## Copyright (C) 2020 Kyligence Inc. All rights reserved.
##
## http://kyligence.io
##
## This software is the confidential and proprietary information of
## Kyligence Inc. ("Confidential Information"). You shall not disclose
## such Confidential Information and shall use it only in accordance
## with the terms of the license agreement you entered into with
## Kyligence Inc.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
## "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
## LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
## A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
## OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
## SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
## LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
## DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
## THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
## (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
## OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
##

source ${KYLIN_HOME}/sbin/prepare-hadoop-env.sh

BYPASS=${SPARK_HOME}/jars/replace-jars-bypass

# replace when has Kerberos, or can't get the value (eg: in FI platform)
kerberosEnabled=`${KYLIN_HOME}/bin/get-properties.sh kylin.kerberos.enabled`
if [[ "${kerberosEnabled}" == "false" || -f ${BYPASS} ]]
then
    return
fi

if [[ $(hadoop version 2>/dev/null) == *"mapr"* ]]
then
    return
fi

echo "Start replacing hadoop jars under ${SPARK_HOME}/jars."

common_jars=
hdfs_jars=
mr_jars=
yarn_jars=
other_jars=

function fi_replace_jars() {
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

    if [[ $(is_fi_c90) == 1 ]]; then
        fi_c90_jars=$(find ${FI_ENV_PLATFORM}/HDFS/hadoop/share/hadoop/common/lib/ -maxdepth 1 \
        -name "stax2-api-*.jar" -o -name "woodstox-core-*.jar" \
        -o -name "commons-configuration2-*.jar" -o -name "htrace-core4-*-incubating.jar" \
        -o -name "re2j-*.jar" -o -name "hadoop-plugins-*.jar" )
    fi
}

function cdp_replace_jars() {
    common_jars=$(find $cdh_mapreduce_path/../hadoop -maxdepth 2 \
    -name "hadoop-annotations-*.jar" -not -name "*test*" \
    -o -name "hadoop-auth-*.jar" -not -name "*test*" \
    -o -name "hadoop-common-*.jar" -not -name "*test*")

    hdfs_jars=$(find $cdh_mapreduce_path/../hadoop-hdfs -maxdepth 1 -name "hadoop-hdfs-*" -not -name "*test*" -not -name "*nfs*")

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

    if [[ $(is_cdh_6_x) == 1 ]]; then
        cdh6_jars=$(find ${cdh_mapreduce_path}/../../jars -maxdepth 1 \
        -name "woodstox-core-*.jar" -o -name "stax2-*.jar" -o -name "commons-configuration2-*.jar" -o -name "re2j-*.jar" )
    fi
}

function hdp3_replace_jars() {
    common_jars=$(find ${hdp_hadoop_path}/ -maxdepth 2 \
    -name "hadoop-annotations-3*.jar" -not -name "*test*" \
    -o -name "hadoop-auth-3*.jar" -not -name "*test*" \
    -o -name "hadoop-common-3*.jar" -not -name "*test*")

    hdp_hadoop_current_path=$(dirname ${hdp_hadoop_path})
    hdfs_jars=$(find ${hdp_hadoop_current_path}/hadoop-hdfs-client/ -maxdepth 1 \
    -name "hadoop-hdfs-3*.jar" -not -name "*test*" \
    -o -name "hadoop-hdfs-httpfs-3*.jar" -not -name "*test*" \
    -o -name "hadoop-hdfs-client-3*.jar" -not -name "*test*" \
    -o -name "hadoop-hdfs-rbf-3*.jar" -not -name "*test*" \
    -o -name "hadoop-hdfs-native-client-3*.jar" -not -name "*test*")

    mr_jars=$(find ${hdp_hadoop_current_path}/hadoop-mapreduce-client/ -maxdepth 1 \
    -name "hadoop-mapreduce-client-app-3*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-common-3*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-jobclient-3*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-shuffle-3*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-core-3*.jar" -not -name "*test*")

    hive_jars=$(find ${hdp_hadoop_current_path}/spark2-client/ -maxdepth 2 \
    -name "hive-exec-1.21*.jar")

    yarn_jars=$(find ${hdp_hadoop_current_path}/hadoop-yarn-client/ -maxdepth 1 \
    -name "hadoop-yarn-api-3*.jar" -not -name "*test*"  \
    -o -name "hadoop-yarn-client-3*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-common-3*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-common-3*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-web-proxy-3*.jar" -not -name "*test*")


    other_jars=$(find ${hdp_hadoop_current_path}/hadoop-client/ -maxdepth 2 \
    -name "htrace-core4*" \
    -o -name "stax2-api-3*.jar" \
    -o -name "woodstox-core-*.jar" \
    -o -name "commons-configuration2-*.jar" \
    -o -name "re2j-*.jar")
}

function tdh_replace_jars() {
    tdh_hadoop_path=${tdh_client_path}/hadoop
    common_jars=$(find ${tdh_hadoop_path}/hadoop/ -maxdepth 2 \
    -name "hadoop-annotations-2*.jar" -not -name "*test*" \
    -o -name "hadoop-auth-2*.jar" -not -name "*test*" \
    -o -name "hadoop-common-2*.jar" -not -name "*test*" \
    -o -name "federation-utils-guardian-3*.jar" -not -name "*test*")

    hdfs_jars=$(find ${tdh_hadoop_path}/hadoop-hdfs/ -maxdepth 1 \
    -name "hadoop-hdfs-2*.jar" -not -name "*test*")

    mr_jars=$(find ${tdh_hadoop_path}/hadoop-mapreduce -maxdepth 1 \
    -name "hadoop-mapreduce-client-app-2*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-common-2*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-jobclient-2*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-shuffle-2*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-core-2*.jar" -not -name "*test*")

    yarn_jars=$(find ${tdh_hadoop_path}/hadoop-yarn/ -maxdepth 1 \
    -name "hadoop-yarn-api-2*.jar" -not -name "*test*"  \
    -o -name "hadoop-yarn-client-2*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-common-2*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-common-2*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-web-proxy-2*.jar" -not -name "*test*")

    other_jars=$(find ${tdh_hadoop_path}/hadoop-hdfs/ -maxdepth 2 \
    -name "htrace-core-3*.jar")
}
find ${tdh_hadoop_path}/hadoop-hdfs/ -maxdepth 2 -name "htrace-core-3*.jar"

function cdh7x_replace_jars() {
  other_cdh7x_jars=$(find ${cdh_hadoop_lib_path}/client/ -maxdepth 1 \
  -name "woodstox-core.jar" \
  -o -name "stax2-api.jar" \
  -o -name "re2j.jar")

  other_jars="${other_jars} ${other_cdh7x_jars}"
}

if [ -n "$FI_ENV_PLATFORM" ]
then
    fi_replace_jars
elif [ -d "$cdh_mapreduce_path" ]
then
    cdp_replace_jars

    if [[ $(is_cdh_7_x) == 1 ]]; then
      cdh7x_replace_jars
    fi
elif [[ $(is_hdp_3_x) == 1 ]]
then
    hdp3_replace_jars
elif [[ $(is_tdh) == 1 ]]
then
    tdh_replace_jars
fi

# not consider HDP

jar_list="${common_jars} ${hdfs_jars} ${mr_jars} ${yarn_jars} ${other_jars} ${cdh6_jars} ${fi_c90_jars}"

echo "Find platform specific jars:${jar_list}, will replace with these jars under ${SPARK_HOME}/jars."

# not in hdp 2.6
if [[  $(is_hdp_2_6) == 0 ]]; then
    find ${SPARK_HOME}/jars -name "htrace-core-*" -exec rm -rf {} \;
    find ${SPARK_HOME}/jars -name "hadoop-*2.6.*.jar" -exec rm -f {} \;
fi

if [[ $(is_hdp_3_x) == 1 ]]; then
  find ${SPARK_HOME}/jars -name "hive-exec-*.jar" -exec rm -f {} \;
  hdp_hadoop_current_path=$(dirname ${hdp_hadoop_path})
  hive_jars=$(find $hdp_hadoop_current_path/spark2-client/ -maxdepth 2 -name "hive-exec-1.21*.jar")
  cp ${hive_jars} ${SPARK_HOME}/jars

  sed -i -r "/hive.execution.engine/I{n; s/tez/mr/}" ${KYLIN_HOME}/hadoop_conf/hive-site.xml
  echo "Change hive.execution.engine to mr finished."
fi

if [[ $(is_cdh_6_x) == 1 ]]; then
    find ${SPARK_HOME}/jars -name "hadoop-hdfs-*.jar" -exec rm -f {} \;
    cp ${SPARK_HOME}/hadoop3/cdh6.1/stax2*.jar ${SPARK_HOME}/jars
fi

find ${SPARK_HOME}/jars -name "htrace-core-*" -exec rm -rf {} \;
find ${SPARK_HOME}/jars -name "hadoop-*2.7.*.jar" -exec rm -f {} \;

for jar_file in ${jar_list}
do
    `cp ${jar_file} ${SPARK_HOME}/jars`
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
