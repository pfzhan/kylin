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

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

if [ $# != 1 ]
then
    echo 'invalid input'
    exit -1
fi

if [[ $CI_MODE == 'true' ]]; then
    cd $dir
    job_jar=$(ls ../../kylin/assembly/target/kylin-*-job.jar)
    tool_jar=$(ls ../../kylin/tool/target/kylin-tool-*.jar|grep -v assembly|grep -v tests)
else
    job_jar=$(ls $KYLIN_HOME/lib/kylin-job-*.jar)
    tool_jar=$(ls $KYLIN_HOME/lib/kylin-tool-*.jar)
fi

kylin_conf_opts=
if [ ! -z "$KYLIN_CONF" ]; then
    kylin_conf_opts="-DKYLIN_CONF=$KYLIN_CONF"
fi
result=`java $kylin_conf_opts -cp $job_jar:$tool_jar org.apache.kylin.tool.KylinConfigCLI $1 2>/dev/null`
echo "$result"