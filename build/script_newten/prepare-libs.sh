#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh
exportProjectVersions

echo "copy lib file"
rm -rf build/lib build/tool
mkdir build/lib build/tool
cp src/assembly/target/kap-assembly-${kap_version}-job.jar build/lib/newten-job.jar
cp src/tool-assembly/target/kap-tool-assembly-${kap_version}-assembly.jar build/tool/kap-tool-${release_version}.jar
cp src/jdbc/target/kap-jdbc-${kap_version}.jar build/lib/kylin-jdbc-kap-${release_version}.jar
cp src/spark-project/kylin-user-session-dep/target/kylin-user-session-dep-${kap_version}.jar build/lib/kylin-user-session-dep-${release_version}.jar

# Copied file becomes 000 for some env (e.g. Cygwin)
#chmod 644 build/lib/kylin-job-kap-${release_version}.jar
#chmod 644 build/lib/kylin-coprocessor-kap-${release_version}.jar
#chmod 644 build/lib/kylin-jdbc-kap-${release_version}.jar
#chmod 644 build/lib/kylin-storage-parquet-kap-${release_version}.jar
#chmod 644 build/tool/kylin-tool-kap-${release_version}.jar