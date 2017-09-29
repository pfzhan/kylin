#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

client_version=1.1.22
# use cdh5.7 version
if [ "$1" == "cdh5.7" ]; then
    pkg_name="kybot-client-${client_version}-cdh5.7-bin.tar.gz"
    pkg_url="http://cn.kyligence.io/download/kybot/${client_version}/kybot-client-${client_version}-cdh5.7-bin.tar.gz"
    pkg_md5="d31244f2a7de66f456352345ecab840a"
fi
# use hbase0.98 version
if [ "$1" == "hbase0.98" ]; then
    pkg_name="kybot-client-${client_version}-hbase0.98-bin.tar.gz"
    pkg_url="http://cn.kyligence.io/download/kybot/${client_version}/kybot-client-${client_version}-hbase0.98-bin.tar.gz"
    pkg_md5="976d4cbf4d1052be5b56bc2abfad68b3"
fi
# use hbase1.x version
if [ "$1" == "hbase1.x" ]; then
    pkg_name="kybot-client-${client_version}-hbase1.x-bin.tar.gz"
    pkg_url="http://cn.kyligence.io/download/kybot/${client_version}/kybot-client-${client_version}-hbase1.x-bin.tar.gz"
    pkg_md5="aaf8a1ca2ebab4178ee9c9e6a848f24c"
fi  

if [[ -z "${pkg_name}" ]]; then
    echo "Client type undefined, supported types: \"hbase1.x\", \"hbase0.98\" and \"cdh5.7\"."
    exit 1;
fi

echo "Checking kybot client ${pkg_name} ..."
pkg_is_available=true
if [ ! -f "build/${pkg_name}" ]
then
    echo "No kybot client binary file."
    pkg_is_available=false
else
    if [ `calMd5 build/${pkg_name} | awk '{print $1}'` != "${pkg_md5}" ]
    then
        echo "md5 check failed"
        rm build/${pkg_name}
        pkg_is_available=false
    fi
fi
if [ $pkg_is_available != true ]; then
    echo "Fetching kybot client..."
    curl --retry 3 -H "Accept: application/octet-stream" -L ${pkg_url} > build/${pkg_name} || { echo "Download kybot client failed"; exit 1; }
fi

echo "Extracting kybot client..."
tar zxf build/${pkg_name} -C build/ || { exit 1; }

echo "Packaging kybot client..."
rm -rf build/kybot
cp -r build/kybot-client-*-bin/kybot build/kybot
cp build/kybot_kap/diag.sh build/kybot/diag.sh
cp build/bin/header.sh build/kybot/header.sh
rm -rf build/kybot-client-*-bin

echo "Registering kybot agent..."
cd build/kybot
mkdir -p WEB-INF/lib
cp kybot-client-agent-runner-lib.jar WEB-INF/lib
jar -uf ../../extensions/server/target/kap-server-${kap_version}.war WEB-INF/lib/kybot-client-agent-runner-lib.jar
rm -rf WEB-INF
cd ../..

# Copied file becomes 000 for some env (e.g. Cygwin)
chmod 644 build/kybot/kybot-client-lib.jar
chmod +x build/kybot/kybot.sh

echo "Kybot client is ready."
