#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh

rm -rf build/influxdb

influxdb_version="1.6.4.x86_64"
influxdb_pkg_md5="e24a00fb3b41d2974f85def72035f9f2"

if [ ! -f "build/influxdb-${influxdb_version}.rpm" ]
then
    echo "no binary file found "
    wget --directory-prefix=build/ https://repos.influxdata.com/centos/6/x86_64/stable/influxdb-${influxdb_version}.rpm || echo "Download influxDB failed"
else
    if [ `calMd5 build/influxdb-${influxdb_version}.rpm | awk '{print $1}'` != "${influxdb_pkg_md5}" ]
    then
        echo "md5 check failed"
        rm build/influxdb-${influxdb_version}.rpm
        wget --directory-prefix=build/ https://repos.influxdata.com/centos/6/x86_64/stable/influxdb-${influxdb_version}.rpm || echo "Download influxDB failed"

    fi
fi

mkdir -p build/influxdb
cp build/influxdb-${influxdb_version}.rpm build/influxdb