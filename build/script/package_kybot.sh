#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh

# Make share commands exist in environment
echo "BUILD STAGE 1 - Checking environment..."
checkCommandExits mvn
checkCommandExits git

if [ ! -d "kylin" ]; then
    echo "Apache Kylin source not found."
    exit 1
fi
exportProjectVersions

kap_commit_sha1=`git rev-parse HEAD`
kylin_commit_sha1=`git submodule status kylin`
kylin_commit_sha1=${kylin_commit_sha1:1:40}
echo "${kap_commit_sha1}@KAP" > build/commit_SHA1
echo "${kylin_commit_sha1}@ApacheKylin" >> build/commit_SHA1
if [ -z "$BUILD_SYSTEM" ]; then
    BUILD_SYSTEM="MANUAL"
fi
echo "Build with ${BUILD_SYSTEM} at" `date "+%Y-%m-%d %H:%M:%S"` >> build/commit_SHA1

echo "BUILD STAGE 2 - Build binaries..."
mvn clean install -DskipTests $@        || { exit 1; }

echo "BUILD STAGE 3 - Prepare and compress package..."
pwd
mkdir dist
cd dist
rm -rf kybot-client-${release_version}-bin
mkdir -p kybot-client-${release_version}-bin/kybot
cp -r ../build/deploy/kybot.sh kybot-client-${release_version}-bin/kybot/
cp ../extensions/tool/target/kap-tool*-kybot.jar kybot-client-${release_version}-bin/kybot/kybot-client-${release_version}.jar
tar -zcvf kybot-client-${release_version}-bin.tar.gz kybot-client-${release_version}-bin
rm -rf kybot-client-${release_version}-bin

cd ..

echo "BUILD FINISHED!"


