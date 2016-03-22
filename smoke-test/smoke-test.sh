#!/bin/bash

PKG_PATH=$1
TARGET_PATH=$2

dir=$(dirname ${0})
cd ${dir}/..
mkdir -p ${TARGET_PATH}

# Setup stage
KYLIN_HOME=${TARGET_PATH}/*kylin*/
if [ -f "${KYLIN_HOME}/pid" ]; then
    PID=`cat ${KYLIN_HOME}/pid`
    if ps -p $PID > /dev/null; then
        echo "Kylin is running, will be killed. (pid=${PID})"
        kill -9 $PID
    fi
fi
rm -rf ${KYLIN_HOME}
tar -zxvf $PKG_PATH -C $TARGET_PATH
${KYLIN_HOME}/bin/kylin.sh start
${KYLIN_HOME}/bin/metastore.sh reset

# Test stage
${KYLIN_HOME}/bin/sample.sh
python smoke-test/testBuildCube.py
python smoke-test/testQuery.py

# Tear down stage
${KYLIN_HOME}/bin/metastore.sh reset
${KYLIN_HOME}/bin/metastore.sh clean --delete true
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.storage.hbase.util.StorageCleanupJob --delete true
${KYLIN_HOME}/bin/kylin.sh stop