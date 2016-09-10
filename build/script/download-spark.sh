#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh

rm -rf build/spark

spark_version="1.6.2"
spark_pkg_md5="604936f2bd8af999d0d624b370f5c4b1"

if [ ! -f "build/spark-${spark_version}-bin-hadoop2.6.tgz" ]
then
    echo "no binary file found"
    wget --directory-prefix=build/ http://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop2.6.tgz || echo "Download spark failed"
else
    if [ `calMd5 build/spark-${spark_version}-bin-hadoop2.6.tgz | awk '{print $1}'` != "${spark_pkg_md5}" ]
    then
        echo "md5 check failed"
        rm build/spark-${spark_version}-bin-hadoop2.6.tgz
        wget --directory-prefix=build/ http://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop2.6.tgz || echo "Download spark failed"

    fi
fi

tar -zxvf build/spark-${spark_version}-bin-hadoop2.6.tgz -C build/   || { exit 1; }
mv build/spark-${spark_version}-bin-hadoop2.6 build/spark

# Remove unused components in Spark
rm -rf build/spark/lib/spark-examples-*
rm -rf build/spark/examples
rm -rf build/spark/data
rm -rf build/spark/python
rm -rf build/spark/R
