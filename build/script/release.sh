#!/bin/bash

# set release version, default value is from pom.xml
if [ -z ${release_version} ]; then
    release_version=2.1-staging
fi
export release_version

dir=$(dirname ${0})
cd ${dir}/../..

echo "KAP Release Version: ${release_version}"
sh build/script/package.sh $@

if [ -f *.license ]; then
    kap_dir=`tar -tf dist/kylin-kap-*-obf.tar.gz|head -1`
    rm -rf $kap_dir
    mkdir $kap_dir
    cp *.license $kap_dir
    gzip -d dist/kylin-kap-*-obf.tar.gz
    tar -uf dist/kylin-kap-*-obf.tar $kap_dir/*.license
    gzip dist/kylin-kap-*-obf.tar
    rm -rf $kap_dir
fi
