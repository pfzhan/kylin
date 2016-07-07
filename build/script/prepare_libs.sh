#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

echo "copy lib file"
rm -rf build/lib
mkdir build/lib build/tool
cp extensions/assembly/target/kap-assembly-${kap_version}-job.jar build/lib/kylin-job-kap-${release_version}.jar
cp extensions/storage-hbase/target/kap-storage-hbase-${kap_version}-coprocessor.jar build/lib/kylin-coprocessor-kap-${release_version}.jar
cp extensions/tool/target/kap-tool-*-assembly.jar build/tool/kap-tool-${release_version}.jar
cp kylin/jdbc/target/kylin-jdbc-${kylin_version}.jar build/lib/kylin-jdbc-kap-${release_version}.jar

# Copied file becomes 000 for some env (e.g. Cygwin)
chmod 644 build/lib/kylin-job-kap-${release_version}.jar
chmod 644 build/lib/kylin-coprocessor-kap-${release_version}.jar
chmod 644 build/lib/kylin-jdbc-kap-${release_version}.jar
chmod 644 build/tool/kap-tool-${release_version}.jar