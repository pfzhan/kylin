#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

sh build/script/prepare_libs.sh || { exit 1; }
sh build/script/prepare_kybot.sh || { exit 1; }

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
