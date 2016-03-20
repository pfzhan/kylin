#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

echo "copy lib file"
rm -rf build/lib
mkdir build/lib
cp kylin/assembly/target/kylin-assembly-${kylin_version}-job.jar build/lib/kylin-job-${kylin_version}.jar
cp extensions/storage-hbase/target/kap-storage-hbase-${kap_version}-coprocessor.jar build/lib/kap-coprocessor-${kap_version}.jar
cp kylin/jdbc/target/kylin-jdbc-${kylin_version}.jar build/lib/kylin-jdbc-${kylin_version}.jar

# Copied file becomes 000 for some env (e.g. Cygwin)
chmod 644 build/lib/kylin-job-${kylin_version}.jar
chmod 644 build/lib/kap-coprocessor-${kap_version}.jar
chmod 644 build/lib/kylin-jdbc-${kylin_version}.jar