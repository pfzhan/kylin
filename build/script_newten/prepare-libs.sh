#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh
exportProjectVersions

echo "copy lib file"
rm -rf build/lib build/tool
mkdir build/lib build/tool
cp src/assembly/target/ke-assembly-${kap_version}-job.jar build/lib/newten-job.jar
cp src/storage-parquet/target/kap-storage-parquet-${kap_version}-spark.jar build/lib/kylin-storage-parquet-kap-${release_version}.jar

# Copied file becomes 000 for some env (e.g. Cygwin)
#chmod 644 build/lib/kylin-job-kap-${release_version}.jar
#chmod 644 build/lib/kylin-coprocessor-kap-${release_version}.jar
#chmod 644 build/lib/kylin-jdbc-kap-${release_version}.jar
#chmod 644 build/lib/kylin-storage-parquet-kap-${release_version}.jar
#chmod 644 build/tool/kylin-tool-kap-${release_version}.jar