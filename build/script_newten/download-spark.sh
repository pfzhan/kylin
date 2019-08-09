#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh

rm -rf build/spark

spark_pkg_name="spark-newten-2.4.1-r13"
spark_pkg_file_name="${spark_pkg_name}.tgz"
spark_pkg_md5="1aba6c5e5f3b94c60b8d2683b2c01805"

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

tar -zxvf build/${spark_pkg_file_name} -C build/   || { exit 1; }
mv build/${spark_pkg_name} build/spark

# Remove unused components in Spark
rm -rf build/spark/lib/spark-examples-*
rm -rf build/spark/examples
rm -rf build/spark/data
rm -rf build/spark/R
