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

spark_pkg_name="spark-newten-3.1.1-4.x-r26"
spark_pkg_file_name="${spark_pkg_name}.tgz"
spark_pkg_md5="e37299fc160bb28496e1b6464a73c941"

checkDownloadSparkVersion ${spark_pkg_name}

if [ ! -f "build/${spark_pkg_file_name}" ]
then
    echo "no binary file found"
    wget --directory-prefix=build/ https://s3.cn-north-1.amazonaws.com.cn/download-resource/kyspark/${spark_pkg_file_name} || echo "Download spark failed"
else
    if [ `calMd5 build/${spark_pkg_file_name} | awk '{print $1}'` != "${spark_pkg_md5}" ]
    then
        echo "md5 check failed"
        rm build/${spark_pkg_file_name}
        wget --directory-prefix=build/ https://s3.cn-north-1.amazonaws.com.cn/download-resource/kyspark/${spark_pkg_file_name}  || echo "Download spark failed"

    fi
fi

tar -zxf build/${spark_pkg_file_name} -C build/   || { exit 1; }
mv build/${spark_pkg_name} build/spark

# Remove unused components in Spark
rm -rf build/spark/lib/spark-examples-*
rm -rf build/spark/examples
rm -rf build/spark/data
rm -rf build/spark/R
