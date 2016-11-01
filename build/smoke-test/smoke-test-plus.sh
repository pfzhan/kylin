#!/bin/bash

PKG_PATH=$1
ORIG_PKG_PATH=$2
TARGET_PATH=$3

cd $(dirname ${0})/..
dir=`pwd`
mkdir -p ${TARGET_PATH}

# Setup stage
KYLIN_PID=`cat "${TARGET_PATH}/kap-*/pid"`
if [ -n "${KYLIN_PID}" ]; then
    if ps -p ${KYLIN_PID} > /dev/null; then
        echo "Kylin is running, will be killed. (pid=${KYILN_PID})"
        kill -9 ${KYLIN_PID}
    fi
fi

rm -rf ${TARGET_PATH}/kap-*
tar -zxvf ${ORIG_PKG_PATH} -C ${TARGET_PATH}

cd ${TARGET_PATH}/kap-*/
export KYLIN_HOME=`pwd`
cd -

${KYLIN_HOME}/bin/metastore.sh reset

# Firstly run origin package to initialize metadata and build a segment
${KYLIN_HOME}/bin/sample.sh
${KYLIN_HOME}/bin/kylin.sh start

echo "Wait 2 minutes for service start KAP orig package."
sleep 2m

# Temp stop spark driver to release resources for yarn
${KYLIN_HOME}/bin/spark_client.sh stop

cd $dir/smoke-test
echo "Start to build on KAP orig package."
python testBuildCube.py 1325376000000 1356998400000     || { exit 1; }
cd -

${KYLIN_HOME}/bin/kylin.sh stop

# Secondly run obfuscated package to read metadata and do incremental build
rm -rf ${TARGET_PATH}/kap-*
tar -zxvf ${PKG_PATH} -C ${TARGET_PATH}

cd ${TARGET_PATH}/kap-*/
export KYLIN_HOME=`pwd`
${KYLIN_HOME}/bin/kylin.sh start

echo "Wait 2 minutes for service start KAP obf package."
sleep 2m

# Temp stop spark driver to release resources for yarn
${KYLIN_HOME}/bin/spark_client.sh stop

cd -

cd $dir/smoke-test
echo "Start to test obf package."
python testBuildCube.py 1356998400000 1456790400000     || { exit 1; }

${KYLIN_HOME}/bin/spark_client.sh start
echo "sleep one minute before exit, allowing spark fully start"
sleep 1m

python testQuery.py                                     || { exit 1; }
${KYLIN_HOME}/bin/spark_client.sh stop

python testDiag.py                                      || { exit 1; }
cd -

# Tear down stage
${KYLIN_HOME}/bin/metastore.sh clean --delete true
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.StorageCleanupJob --delete true
${KYLIN_HOME}/bin/metastore.sh reset
${KYLIN_HOME}/bin/kylin.sh stop

echo "Finished!"