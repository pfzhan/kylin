#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh

rm -rf build/grafana

grafana_version="6.2.4"
grafana_pkg_md5="b0c5b1085db30914e128980b5fe5f553"

if [ ! -f "build/grafana-${grafana_version}.linux-amd64.tar.gz" ]
then
    echo "no binary file found "
    wget --directory-prefix=build/ https://dl.grafana.com/oss/release/grafana-${grafana_version}.linux-amd64.tar.gz || echo "Download grafana failed"
else
    if [ `calMd5 build/grafana-${grafana_version}.linux-amd64.tar.gz | awk '{print $1}'` != "${grafana_pkg_md5}" ]
    then
        echo "md5 check failed"
        rm build/grafana-${grafana_version}.linux-amd64.tar.gz
        wget --directory-prefix=build/ https://dl.grafana.com/oss/release/grafana-${grafana_version}.linux-amd64.tar.gz || echo "Download grafana failed"
    fi
fi

tar -zxvf build/grafana-${grafana_version}.linux-amd64.tar.gz -C build/ || { exit 1; }
mv build/grafana-${grafana_version} build/grafana
rm -rf build/grafana/conf/provisioning