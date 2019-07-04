#!/bin/bash

# set release version, default value is from pom.xml
if [ -z ${release_version} ]; then
    release_version='staging' # since every release has a version, it's almost useless
fi
export release_version

dir=$(dirname ${0})
cd ${dir}/../..

echo "Kyligence Enterprise Release Version: ${release_version}"
sh build/script_newten/package.sh $@

if [ -f LICENSE ]; then
    for package_name in `ls dist/Kyligence-Enterprise-*.tar.gz`; do
        kap_dir=`tar -tf ${package_name}|head -1`
        rm -rf $kap_dir
        mkdir $kap_dir
        cp LICENSE $kap_dir
        gzip -d ${package_name}
        tar_name=`ls dist/Kyligence-Enterprise*.tar`
        tar -uf ${tar_name} $kap_dir/LICENSE
        gzip ${tar_name}
        rm -rf $kap_dir
    done
fi