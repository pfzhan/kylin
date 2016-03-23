#!/bin/bash

# This tool accepts two parameters, the first is tar package of kylin, the second is target path for decompress, which
# will contain KYLIN_HOME.
#
# Usage: bash smoke-test.sh ../dist/apache-kylin-1.5.0-bin.tar.gz ../dist/apache-kylin-1.5.0-bin
#
# The process of smoke test is based on sample metadata & data.
# 1. run sample.sh to load sample data
# 2. use rest API to build cube
# 3. use rest API to execute some SQL statements, which locates under sql directory
# 4. compare query result with result file under sql directory

PKG_PATH=$1
TARGET_PATH=$2

cd $(dirname ${0})/..
dir=`pwd`
mkdir -p ${TARGET_PATH}
rm -rf ${TARGET_PATH}/*kylin*

# Setup stage
tar -zxvf $PKG_PATH -C $TARGET_PATH
cd ${TARGET_PATH}/*kylin*/
KYLIN_HOME=`pwd`
cd -
if [ -f "${KYLIN_HOME}/pid" ]; then
    PID=`cat ${KYLIN_HOME}/pid`
    if ps -p $PID > /dev/null; then
        echo "Kylin is running, will be killed. (pid=${PID})"
        kill -9 $PID
    fi
fi
${KYLIN_HOME}/bin/metastore.sh reset

# Test stage
${KYLIN_HOME}/bin/sample.sh

${KYLIN_HOME}/bin/kylin.sh start

cd smoke-test
python testBuildCube.py     || { exit 1; }
python testQuery.py         || { exit 1; }
cd ..

# Tear down stage
${KYLIN_HOME}/bin/metastore.sh clean --delete true
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.storage.hbase.util.StorageCleanupJob --delete true
${KYLIN_HOME}/bin/metastore.sh reset
${KYLIN_HOME}/bin/kylin.sh stop