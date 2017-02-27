#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

echo "Fetching kybot client..."
curl -u Kyligence:511e7eaf39667ef98826be54e7ff0447c9494cf1 -H "Accept: application/octet-stream" -L https://api.github.com/repos/Kyligence/kybot-client/releases/assets/3282956 > kybot-client.tar.gz
tar zxf kybot-client.tar.gz

echo "Preparing kybot client..."
rm -rf build/kybot
cp -r kybot-client-*-bin/kybot build/kybot
cp build/kybot_kap/diag.sh build/kybot/diag.sh
cp build/bin/header.sh build/kybot/header.sh
rm -rf kybot-client-*-bin

# Copied file becomes 000 for some env (e.g. Cygwin)
chmod 644 build/kybot/kybot-client-lib.jar
chmod +x build/kybot/kybot.sh

echo "Kybot client is ready."
