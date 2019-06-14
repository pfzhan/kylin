#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh

tpch_pkg_name="tpch-benchmark"
tpch_pkg_file_name="tpch-benchmark-v1.tgz"
tpch_pkg_md5="2da598fe3a36418fe658e8356935e686"

if [ ! -f "build/${tpch_pkg_file_name}" ]
then
    echo "no binary file found"
    wget --directory-prefix=build/ https://download-resource.s3.cn-north-1.amazonaws.com.cn/tpch_benchmark/${tpch_pkg_file_name} || echo "Download tpch benchmark tool failed."
else
    if [ `calMd5 build/${tpch_pkg_file_name} | awk '{print $1}'` != "${tpch_pkg_md5}" ]
    then
        echo "md5 check failed"
        rm build/${tpch_pkg_file_name}
        wget --directory-prefix=build/ https://download-resource.s3.cn-north-1.amazonaws.com.cn/tpch_benchmark/${tpch_pkg_file_name} || echo "Download tpch benchmark tool failed."
    fi
fi

tar -zxvf build/${tpch_pkg_file_name} -C build/   || { exit 1; }
