#!/bin/bash

##
## Copyright (C) 2020 Kyligence Inc. All rights reserved.
##
## http://kyligence.io
##
## This software is the confidential and proprietary information of
## Kyligence Inc. ("Confidential Information"). You shall not disclose
## such Confidential Information and shall use it only in accordance
## with the terms of the license agreement you entered into with
## Kyligence Inc.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
## "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
## LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
## A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
## OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
## SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
## LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
## DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
## THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
## (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
## OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
##

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh
source ${KYLIN_HOME}/sbin/prepare-mrs-env.sh
if [[ -d "/data/external-catalog" ]];then
    plugin_version=`${KYLIN_HOME}/bin/get-properties.sh kylin.datasource.external-catalog.version`
    cp -rf /data/external-catalog/$plugin_version/*.jar ${KYLIN_HOME}/spark/jars/
    source /data/external-catalog/$plugin_version/setenv.sh
fi

if [[ `isValidJavaVersion` == "false" ]]; then
    quit "ERROR: Java 1.8 or above is required for Kyligence Enterprise"
fi

if [[ -f "${KYLIN_HOME}/conf/setenv.sh" ]]; then
    source ${KYLIN_HOME}/conf/setenv.sh
fi

if [[ -d "/data/external-jars" ]];then
    cp -rf /data/external-jars/*.jar ${KYLIN_HOME}/spark/jars/
fi

export JAVA_VM_XMS=${JAVA_VM_XMS:-1g}
export JAVA_VM_XMX=${JAVA_VM_XMX:-8g}

export JAVA_VM_TOOL_XMS=${JAVA_VM_TOOL_XMS:-${JAVA_VM_XMS}}
export JAVA_VM_TOOL_XMX=${JAVA_VM_TOOL_XMX:-${JAVA_VM_XMX}}

export KYLIN_EXTRA_START_OPTS=""
export KYLIN_JVM_SETTINGS=${KYLIN_JVM_SETTINGS:-"-server -Xms${JAVA_VM_XMS} -Xmx${JAVA_VM_XMX} -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=16m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark  -Xloggc:${KYLIN_HOME}/logs/kylin.gc.%p  -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=64M -XX:-OmitStackTraceInFastThrow -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector -DAsyncLogger.RingBufferSize=8192"}

# add some must have settings
if [[ ${KYLIN_JVM_SETTINGS} != *"-DAsyncLogger.RingBufferSize="* ]]
then
 export KYLIN_JVM_SETTINGS="${KYLIN_JVM_SETTINGS} -DAsyncLogger.RingBufferSize=8192"
fi

if [ -n "$IS_MRS_PLATFORM" ]
then
 export KYLIN_JVM_SETTINGS="${KYLIN_JVM_SETTINGS} -Dzookeeper.sasl.client=false"
fi


# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode. Tune the variable down to prevent vmem explosion.
# See HADOOP-7154.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

verbose "KYLIN_JVM_SETTINGS is ${KYLIN_JVM_SETTINGS}"
KYLIN_EXTRA_START_OPTS="${KYLIN_JVM_SETTINGS} ${KYLIN_EXTRA_START_OPTS}"

if [[ ! -z "${KYLIN_DEBUG_SETTINGS}" ]]
then
    verbose "KYLIN_DEBUG_SETTINGS is ${KYLIN_DEBUG_SETTINGS}"
    KYLIN_EXTRA_START_OPTS="${KYLIN_DEBUG_SETTINGS} ${KYLIN_EXTRA_START_OPTS}"
else
    verbose "KYLIN_DEBUG_SETTINGS is not set, will not enable remote debuging"
fi

if [[ ! -z "${KYLIN_LD_LIBRARY_SETTINGS}" ]]
then
    verbose "KYLIN_LD_LIBRARY_SETTINGS is ${KYLIN_LD_LIBRARY_SETTINGS}"
    KYLIN_EXTRA_START_OPTS="${KYLIN_LD_LIBRARY_SETTINGS} ${KYLIN_EXTRA_START_OPTS}"
else
    verbose "KYLIN_LD_LIBRARY_SETTINGS is not set, it is okay unless you want to specify your own native path"
fi
