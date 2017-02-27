#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

sh build/script/prepare_libs.sh || { exit 1; }

for PARAM in $@; do
    if [ "$PARAM" == "cdh5.7" ]; then
        CDH=1
        break
    fi
done

if [ "$CDH" == "1" ]; then
    sh build/script/prepare_kybot_cdh.sh || { exit 1; }
else
	sh build/script/prepare_kybot.sh || { exit 1; }
fi

if [ "$SKIP_OBF" != "1" ]; then
    build/script/obfuscate.sh       || { exit 1; }
fi

cp extensions/server/target/kap-server-${kap_version}.war build/tomcat/webapps/kylin.war
chmod 644 build/tomcat/webapps/kylin.war

echo "Start to add js & css to war..."
if [ ! -d "webapp/dist" ]
then
    echo "Failed to generate js files!"
    exit 1
fi

cd webapp/dist
for f in * .[!.]*
do
    echo "Adding $f to war"
    jar -uf ../../build/tomcat/webapps/kylin.war $f
    if [ "$SKIP_OBF" != "1" ]; then
        jar -uf ../../tmp/kylin.war $f
    fi
done