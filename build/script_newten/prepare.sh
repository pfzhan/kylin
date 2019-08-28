#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..


source build/script_newten/functions.sh
exportProjectVersions

sh build/script_newten/prepare-libs.sh || { exit 1; }

for PARAM in $@; do
    if [ "$PARAM" == "cdh5.7" ]; then
        CDH=1
        break
    fi
done

if [ "$SKIP_OBF" != "1" ]; then
    build/script_newten/obfuscate.sh       || { exit 1; }
fi

#create ext dir
mkdir -p build/ext

mkdir -p build/server
chmod -R 755 build/server

cp src/server/target/kap-server-${kap_version}.jar build/server/newten.jar
cp -r src/server/target/jars build/server/

echo "Start to add js & css..."
if [ ! -d "kystudio/dist" ]
then
    echo "Failed to generate js files!"
    exit 1
fi

cd kystudio
mkdir -p ../build/server/webapp

cp -rf ./dist ../build/server/webapp
