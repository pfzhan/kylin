#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

echo "Building kybot client..."
rm -rf build/kybot
mkdir build/kybot
cp kybot/core/target/kybot-client-core-*-assembly.jar build/kybot/kybot-client-${release_version}.jar
cp kybot/build/bin/kybot.sh build/kybot/kybot.sh
cp kybot/build/kap/diag.sh build/kybot/diag.sh

# Copied file becomes 000 for some env (e.g. Cygwin)
chmod 644 build/kybot/kybot-client-${release_version}.jar
chmod +x build/kybot/kybot.sh