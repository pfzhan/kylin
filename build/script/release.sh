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
    package_name=`ls dist/kap-*.tar.gz|grep -v orig.tar.gz$`
    kap_dir=`tar -tf ${package_name}|head -1`
    rm -rf $kap_dir
    mkdir $kap_dir
    cp *.license $kap_dir
    gzip -d ${package_name}
    tar_name=`ls dist/kap-*.tar|grep -v orig.tar$`
    tar -uf ${tar_name} $kap_dir/*.license
    gzip ${tar_name}
    rm -rf $kap_dir
fi
