#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh

# Make share commands exist in environment
checkCommandExits mvn
checkCommandExits git
checkCommandExits npm

if [ ! -d "kylin" ];then
    echo "Apache Kylin source not found."
    exit 1
fi
exportProjectVersions

kap_commit_sha1=`git rev-parse HEAD`
kylin_commit_sha1=`git submodule status kylin`
kylin_commit_sha1=${kylin_commit_sha1:1:40}
echo "${kap_commit_sha1}@KAP" > build/commit_SHA1
echo "${kylin_commit_sha1}@ApacheKylin" >> build/commit_SHA1

# Start to package
sh build/script/download-tomcat.sh || { exit 1; }
sh build/script/build.sh || { exit 1; }
sh build/script/prepare.sh || { exit 1; }
sh build/script/compress.sh || { exit 1; }



