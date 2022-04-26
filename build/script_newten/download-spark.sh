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

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh

rm -rf build/spark

#spark_version_pom=`mvn help:evaluate -Dexpression=spark.version | grep -E '^[0-9]+\.[0-9]+\.[0-9]+' `
#spark_pkg_name=spark-newten-"`echo ${spark_version_pom}| sed "s/-kylin//g"`"
#spark_pkg_file_name="${spark_pkg_name}.tgz"
# TODO Temporarily workaround for spark_on_k8s. The spark repo is https://github.com/xifeng/spark/commits/yinglong-spark
spark_pkg_name="spark-newten-3.2.0-4.x-r60-yinglong-beta1"
spark_pkg_file_name="${spark_pkg_name}.tgz"
spark_pkg_md5="ea1def687a40d2728dd6642459e56ee4"


if [ ! -f "build/${spark_pkg_file_name}" ]
then
    echo "no binary file found"
    wget --directory-prefix=build/ https://s3.cn-north-1.amazonaws.com.cn/download-resource/kyspark/${spark_pkg_file_name} || echo "Download spark failed"
fi

mkdir -p  build/${spark_pkg_name}
tar -zxf build/${spark_pkg_file_name} -C build/${spark_pkg_name} --strip-components 1 || { exit 1; }
mv build/${spark_pkg_name} build/spark

# Remove unused components in Spark
rm -rf build/spark/lib/spark-examples-*
rm -rf build/spark/examples
rm -rf build/spark/data
rm -rf build/spark/R
rm -rf build/spark/hive_1_2_2

cp -rf build/hadoop3 build/spark/

if [[ "${WITH_HIVE1}" != "0" ]]; then
    if [ ! -f "build/hive_1_2_2.tar.gz" ]
    then
        echo "no binary file found"
        wget --directory-prefix=build/ https://s3.cn-north-1.amazonaws.com.cn/download-resource/kyspark/hive_1_2_2.tar.gz || echo "Download hive1 failed"
    else
        if [ `calMd5 build/hive_1_2_2.tar.gz | awk '{print $1}'` != "e8e86e086fb7e821d724ad0c19457a36" ]
        then
            echo "md5 check failed"
            rm build/hive_1_2_2.tar.gz
            wget --directory-prefix=build/ https://s3.cn-north-1.amazonaws.com.cn/download-resource/kyspark/hive_1_2_2.tar.gz  || echo "Download hive1 failed"
        fi
    fi
    tar -zxf build/hive_1_2_2.tar.gz -C build/spark/ || { exit 1; }
fi
