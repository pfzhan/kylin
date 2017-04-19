#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

# use hbase1.x version
if [ "$1" == "hbase1.x" ]; then
    pkg_name="kybot-client-1.1.2-hbase1.x-bin.tar.gz"
    pkg_asset_id="3681576"
    pkg_md5="f76dac35a959f4f7cb17663caef2b8ef"
fi  
# use hbase0.98 version
if [ "$1" == "hbase0.98" ]; then
    pkg_name="kybot-client-1.1.2-hbase0.98-bin.tar.gz"
    pkg_asset_id="3680319"
    pkg_md5="6c0044487c33ec8e2a3b2251868380e6"
fi
# use cdh5.7 version
if [ "$1" == "cdh5.7" ]; then
    pkg_name="kybot-client-1.1.2-cdh5.7-bin.tar.gz"
    pkg_asset_id="3681605"
    pkg_md5="5e61d2e014871be231c7f5af7c230a45"
fi

if [[ -z "${pkg_name}" ]]; then
    echo "Client type undefined, supported types: \"hbase1.x\", \"hbase0.98\" and \"cdh5.7\"."
    exit 1;
fi

github_token="Kyligence:511e7eaf39667ef98826be54e7ff0447c9494cf1"

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
    curl --retry 3 -u ${github_token} -H "Accept: application/octet-stream" -L https://api.github.com/repos/Kyligence/kybot-client/releases/assets/${pkg_asset_id} > build/${pkg_name} || { echo "Download kybot client failed"; exit 1; }
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
