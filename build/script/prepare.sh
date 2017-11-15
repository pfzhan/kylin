#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..


source build/script/functions.sh
exportProjectVersions

sh build/script/prepare-libs.sh || { exit 1; }

for PARAM in $@; do
    if [ "$PARAM" == "cdh5.7" ]; then
        CDH=1
        break
    fi
done

if [ "$CDH" == "1" ]; then
    sh build/script/prepare-kybot.sh cdh5.7 || { exit 1; }
else
    sh build/script/prepare-kybot.sh hbase1.x || { exit 1; }
fi

if [ "$SKIP_OBF" != "1" ]; then
    build/script/obfuscate.sh       || { exit 1; }
fi

#create ext dir
mkdir -p build/ext

cp extensions/server/target/kap-server-${kap_version}.war build/tomcat/webapps/kylin.war
chmod 644 build/tomcat/webapps/kylin.war

echo "Start to add js & css to war..."
if [ ! -d "kystudio/dist" ]
then
    echo "Failed to generate js files!"
    exit 1
fi

cd kystudio/dist
for f in *
do
    echo "Adding $f to war"
    jar -uf ../../build/tomcat/webapps/kylin.war $f
    if [ "$SKIP_OBF" != "1" ]; then
        jar -uf ../../tmp/kylin.war $f
    fi
done