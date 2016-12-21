#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh

if [ "$1" == "-skipObf" ] || [ "$2" == "-skipObf" ]; then
    export SKIP_OBF=1
    echo "Skip Obfuscation..."
fi

if [ "$1" == "-noPlus" ] || [ "$2" == "-noPlus" ]; then
    export PACKAGE_PLUS=0
    echo "Packing for KAP Normal..."
else
	export PACKAGE_PLUS=1
	echo "Packing for KAP Plus..."
fi

# Make share commands exist in environment
echo "BUILD STAGE 1 - Checking environment..."
checkCommandExits mvn
checkCommandExits git
checkCommandExits npm

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
sh build/script/build.sh $@             || { exit 1; }

echo "BUILD STAGE 3 - Prepare tomcat..."
sh build/script/download-tomcat.sh      || { exit 1; }
if [ "${PACKAGE_PLUS}" != "0" ]; then
    echo "BUILD STAGE 4 - Prepare spark..."
    sh build/script/download-spark.sh      || { exit 1; }
else
    echo "BUILD STAGE 4 - Skip spark for KAP Normal"
fi

echo "BUILD STAGE 5 - Prepare and compress package..."
sh build/script/prepare.sh              || { exit 1; }
sh build/script/compress.sh             || { exit 1; }

echo "BUILD FINISHED!"
