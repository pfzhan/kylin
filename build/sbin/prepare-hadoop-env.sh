#!/bin/bash
# Kyligence Inc. License

cdh_mapreduce_path=$CDH_MR2_HOME

if [[ -z ${cdh_mapreduce_path} ]]
then
    if [[ -d "/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce" ]]
    then
        cdh_mapreduce_path="/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce"
    else
        cdh_mapreduce_path="/usr/lib/hadoop-mapreduce"
    fi
fi

function isCDH_6_1() {
    hadoop_common_file=`find ${cdh_mapreduce_path}/../../jars/ -maxdepth 1 -name "hadoop-common-*.jar" -not -name "*test*" | tail -1`
    cdh_version=${hadoop_common_file##*/}

    if [[ "${cdh_version}" == hadoop-common-3.*-cdh6.1* ]]; then
        echo 1
        return 1
    fi

    echo 0
    return 0
}

hdp_hadoop_path=$HDP_HADOOP_HOME
if [[ -z ${hdp_hadoop_path} ]]
then
    if [[ -d "/usr/hdp/current/hadoop-client" ]]; then
        hdp_hadoop_path="/usr/hdp/current/hadoop-client"
    else
        hdp_hadoop_path="/usr/hdp/*/hadoop"
    fi
fi

function isHDP_3_1() {
    hadoop_common_file="`find ${hdp_hadoop_path}/ -maxdepth 1 -name hadoop-common-*.jar -not -name *test* | tail -1`"
    hdp_version=${hadoop_common_file##*/}

    if [[ "${hdp_version}" == hadoop-common-3.1* ]]; then
        echo 1
        return 1
    fi

    echo 0
    return 0
}

function isFI_C90() {
    ## FusionInsight platform C70/C90.
    if [[ -n "$BIGDATA_CLIENT_HOME" ]]; then
        hadoop_common_file="`find ${BIGDATA_CLIENT_HOME}/HDFS/hadoop/share/hadoop/common/ -maxdepth 1 -name hadoop-common-*.jar -not -name *test* | tail -1`"
        fi_version=${hadoop_common_file##*/}

        if [[ "${fi_version}" == hadoop-common-3.1* ]]; then
            echo 1
            return 1
        fi
    fi

    echo 0
    return 0
}