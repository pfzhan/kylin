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

source ${KYLIN_HOME}/sbin/replace-jars-under-spark.sh

function configureMaprSparkSql() {
    # copy hive-conf/hive-site.xml to $SPARK_HOME/conf/ and delete "hive.execution.engine" property
    if [ -z ${kylin_hadoop_conf_dir} ]; then
        source ${KYLIN_HOME}/sbin/prepare-hadoop-conf-dir.sh
    fi

    hive_conf_spark_path="${SPARK_HOME}/conf/hive-site.xml"
    if [ -f ${hive_conf_spark_path} ]; then
        rm -f ${hive_conf_spark_path}
    fi
    cp ${kylin_hadoop_conf_dir}/hive-site.xml ${hive_conf_spark_path}
    match_line=`grep -n "<name>hive.execution.engine</name>" ${hive_conf_spark_path}|cut -d ":" -f 1`
    if [ -n "${match_line}" ]; then
        start_line=$(($match_line-1))
        end_line=$(($match_line+2))
        sed -i "${start_line}"','"${end_line}"'d' ${hive_conf_spark_path}
    fi

    # configure "spark.yarn.dist.files" in spark-defaults.conf
    config_spark_default_str="spark.yarn.dist.files"
    spark_defaults_conf_path="${SPARK_HOME}/conf/spark-defaults.conf"
    if [ ! -f ${spark_defaults_conf_path} ]; then
        touch ${spark_defaults_conf_path}
    fi
    match_spark_default_line=`grep -n "${config_spark_default_str}" ${spark_defaults_conf_path}|cut -d ":" -f 1`
    if [ -n "${match_spark_default_line}" ]; then
        sed -i "${match_spark_default_line}"'d' ${spark_defaults_conf_path}
    fi
    echo "${config_spark_default_str}     ${hive_conf_spark_path}"  >> ${spark_defaults_conf_path}

    # export SAPRK_HOME in spark-env.sh
    config_spark_env_str="export SPARK_HOME=${SPARK_HOME}"
    spark_env_sh_path="${SPARK_HOME}/conf/spark-env.sh"
    if [ -f ${spark_env_sh_path} ]; then
        match_spark_env_line=`grep -n "${config_spark_env_str}" ${spark_env_sh_path}|cut -d ":" -f 1`
        if [ -n "${match_spark_env_line}" ]; then
            sed -i "${match_spark_env_line}"'d' ${spark_env_sh_path}
        fi
        echo "${config_spark_env_str}" >> ${spark_env_sh_path}
    fi
}

# check prepare spark

if [[ $(hadoop version) == *"mapr"* ]]
then
    SPARK_HDP_VERSION="mapr"
else
    if [ -f /etc/cloudversion ]
    then
        case $(cat /etc/cloudversion) in
            *"aliyun"*)
            {
                SPARK_HDP_VERSION="aliyun"
            }
            ;;
            *"azure"*)
            {
                SPARK_HDP_VERSION="azure"
            }
            ;;
            *"aws"*)
            {
                SPARK_HDP_VERSION="aws"
            }
            ;;
            *)
            {
                SPARK_HDP_VERSION="hadoop"
            }
        esac
    else
        SPARK_HDP_VERSION="hadoop"
    fi
fi
verbose "SPARK_HDP_VERSION is set to '${SPARK_HDP_VERSION}'"

## MapR is a different operation rule

if [ ! -f ${KYLIN_HOME}/spark/spark_hdp_version ]
then
    echo "hadoop" > ${KYLIN_HOME}/spark/spark_hdp_version
fi
if [[ $(cat ${KYLIN_HOME}/spark/spark_hdp_version) != *"${SPARK_HDP_VERSION}"* ]]
then
    cp -rf ${KYLIN_HOME}/spark ${KYLIN_HOME}/spark_backup
    case $SPARK_HDP_VERSION in
        "mapr")
        {
            if [ ! -d "$SPARK_HOME" ]
            then
                quit 'Please make sure SPARK_HOME has been set (export as environment variable first)'
            fi
            cp -rf ${SPARK_HOME} ${KYLIN_HOME}/spark-tmp
            rm -rf ${KYLIN_HOME}/spark-tmp/jars/spark-catalyst*.jar
            rm -rf ${KYLIN_HOME}/spark-tmp/jars/spark-sql*.jar
            cp -rf ${KYLIN_HOME}/spark/jars/spark-sql*.jar ${KYLIN_HOME}/spark-tmp/jars/
            cp -rf ${KYLIN_HOME}/spark/jars/spark-catalyst*.jar ${KYLIN_HOME}/spark-tmp/jars/
            rm -rf ${KYLIN_HOME}/spark
            mv ${KYLIN_HOME}/spark-tmp ${KYLIN_HOME}/spark
        }
        ;;
        "aliyun")
        {
            cp /usr/lib/hadoop-current/lib/hadoop-lzo-*.jar ${KYLIN_HOME}/spark/jars/
            cp /usr/lib/hadoop-current/share/hadoop/tools/lib/hadoop-aliyun-*.jar ${KYLIN_HOME}/spark/jars/
            cp /usr/lib/spark-current/jars/aliyun-sdk-oss-*.jar ${KYLIN_HOME}/spark/jars/
            cp /usr/lib/hadoop-current/share/hadoop/tools/lib/jdom-*.jar ${KYLIN_HOME}/spark/jars/
        }
        ;;
        "azure")
        {
            rm -rf ${KYLIN_HOME}/spark/jars/hadoop-*
            cp /usr/hdp/current/spark2-client/jars/hadoop-* ${KYLIN_HOME}/spark/jars/
            cp /usr/hdp/current/spark2-client/jars/azure* ${KYLIN_HOME}/spark/jars/
            cp /usr/hdp/current/spark2-client/jars/aws* ${KYLIN_HOME}/spark/jars/
            cp /usr/hdp/current/spark2-client/jars/hadoop-aws-*.jar ${KYLIN_HOME}/spark/jars/
        }
        ;;
        "aws")
        {
            rm -rf ${KYLIN_HOME}/spark/jars/hadoop-*
            cp /usr/lib/hadoop/hadoop*amzn*.jar ${KYLIN_HOME}/spark/jars/
            cp /usr/lib/hadoop-hdfs/hadoop*amzn*.jar ${KYLIN_HOME}/spark/jars/
            cp /usr/lib/hadoop-mapreduce/hadoop*amzn*.jar ${KYLIN_HOME}/spark/jars/
            cp /usr/lib/hadoop-yarn/hadoop*amzn*.jar ${KYLIN_HOME}/spark/jars/
        }
        ;;
    esac
    echo "${SPARK_HDP_VERSION}" > ${KYLIN_HOME}/spark/spark_hdp_version

    echo "${SPARK_HDP_VERSION} Spark jars exchange SUCCESS"
    echo "SPARK_HDP_VERSION save in PATH ${KYLIN_HOME}/spark/spark_hdp_version"
fi

export SPARK_HOME=${KYLIN_HOME}/spark
verbose "Export SPARK_HOME to ${KYLIN_HOME}/spark"

if [ "${SPARK_HDP_VERSION}" = "mapr" ]; then
    configureMaprSparkSql
fi