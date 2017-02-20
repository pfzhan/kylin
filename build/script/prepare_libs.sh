#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

echo "copy lib file"
rm -rf build/lib
mkdir build/lib
cp extensions/assembly/target/kap-assembly-${kap_version}-job.jar build/lib/kylin-job-kap-${release_version}.jar
cp extensions/storage-hbase/target/kap-storage-hbase-${kap_version}-coprocessor.jar build/lib/kylin-coprocessor-kap-${release_version}.jar
cp extensions/tool-assembly/target/kap-tool-assembly-${kap_version}-assembly.jar build/lib/kylin-tool-kap-${release_version}.jar
cp extensions/storage-parquet/target/kap-storage-parquet-${kap_version}-spark.jar build/lib/kylin-storage-parquet-kap-${release_version}.jar
cp kylin/jdbc/target/kylin-jdbc-${kylin_version}.jar build/lib/kylin-jdbc-kap-${release_version}.jar

# Copied file becomes 000 for some env (e.g. Cygwin)
chmod 644 build/lib/kylin-job-kap-${release_version}.jar
chmod 644 build/lib/kylin-coprocessor-kap-${release_version}.jar
chmod 644 build/lib/kylin-jdbc-kap-${release_version}.jar
chmod 644 build/lib/kylin-storage-parquet-kap-${release_version}.jar
chmod 644 build/lib/kylin-tool-kap-${release_version}.jar