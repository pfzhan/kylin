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
