#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# source me


JAVA_VERSION=`java -version 2>&1 | awk -F\" '/version/ {print $2}'`

if [[ $JAVA_VERSION ]] && [[ "$JAVA_VERSION" > "1.8" ]]; then
    export KYLIN_JVM_SETTINGS="-server -Xms1g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=16m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark  -Xloggc:$KYLIN_HOME/logs/kylin.gc.$$  -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=64M"
else
    quit "Java 1.8 or above is required."
fi

# uncomment following to for it to take effect(the values need adjusting to fit your env)
# export KYLIN_DEBUG_SETTINGS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

# uncomment following to for it to take effect(the values need adjusting to fit your env)
# export KYLIN_LD_LIBRARY_SETTINGS="-Djava.library.path=/apache/hadoop/lib/native/Linux-amd64-64"

# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode. Tune the variable down to prevent vmem explosion.
# See HADOOP-7154.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

export KYLIN_EXTRA_START_OPTS=""

if [ ! -z "${KYLIN_JVM_SETTINGS}" ]
then
    verbose "KYLIN_JVM_SETTINGS is ${KYLIN_JVM_SETTINGS}"
    KYLIN_EXTRA_START_OPTS="${KYLIN_JVM_SETTINGS} ${KYLIN_EXTRA_START_OPTS}"
else
    verbose "KYLIN_JVM_SETTINGS is not set, using default jvm settings: ${KYLIN_JVM_SETTINGS}"
fi

if [ ! -z "${KYLIN_DEBUG_SETTINGS}" ]
then
    verbose "KYLIN_DEBUG_SETTINGS is ${KYLIN_DEBUG_SETTINGS}"
    KYLIN_EXTRA_START_OPTS="${KYLIN_DEBUG_SETTINGS} ${KYLIN_EXTRA_START_OPTS}"
else
    verbose "KYLIN_DEBUG_SETTINGS is not set, will not enable remote debuging"
fi

if [ ! -z "${KYLIN_LD_LIBRARY_SETTINGS}" ]
then
    verbose "KYLIN_LD_LIBRARY_SETTINGS is ${KYLIN_LD_LIBRARY_SETTINGS}"
    KYLIN_EXTRA_START_OPTS="${KYLIN_LD_LIBRARY_SETTINGS} ${KYLIN_EXTRA_START_OPTS}"
else
    verbose "KYLIN_LD_LIBRARY_SETTINGS is not set, it is okay unless you want to specify your own native path"
fi
