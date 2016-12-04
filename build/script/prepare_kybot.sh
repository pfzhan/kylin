#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

echo "Building kybot client..."
rm -rf build/kybot
mkdir build/kybot
cp kybot/core-161/target/kybot-client-core-*-assembly.jar build/kybot/kybot-client-lib.jar
cp kybot/build/bin/kybot.sh build/kybot/kybot.sh
cp kybot/build/conf/kybot-client.properties build/kybot/kybot-client.properties
cp kybot/build/kap/diag.sh build/kybot/diag.sh
cp build/bin/header.sh build/kybot/header.sh

# Copied file becomes 000 for some env (e.g. Cygwin)
chmod 644 build/kybot/kybot-client-lib.jar
chmod +x build/kybot/kybot.sh