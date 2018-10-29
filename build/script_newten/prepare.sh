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

#create ext dir
mkdir -p build/ext

mkdir -p build/server

cp src/server/target/kap-server-${kap_version}.jar build/server/newten.jar
chmod 644 build/server/newten.jar

echo "Start to add js & css to war..."
if [ ! -d "kystudio/dist" ]
then
    echo "Failed to generate js files!"
    exit 1
fi

cd kystudio
mkdir -p build/server/webapp

cp -rf ./dist ../build/server/webapp
