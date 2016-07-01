#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

sh build/script/prepare_libs.sh || { exit 1; }
build/script/obfuscate.sh       || { exit 1; }

cp extensions/server/target/kap-server-${kap_version}.war build/tomcat/webapps/kylin.war
chmod 644 build/tomcat/webapps/kylin.war

# rename obf war
mv tmp/kap-server-${kap_version}.war tmp/kylin.war
chmod 644 tmp/kylin.war

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
    jar -uf ../../tmp/kylin.war $f
done
