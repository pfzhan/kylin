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

dir=$(dirname ${0})
cd ${dir}/..

set -o pipefail
rm testall*.log

#export SPARK_HOME=/root/spark-1.6.2-bin-hadoop2.6
export CI_MODE=true
export hdpv="2.2.4.2-2"

mvn -f extensions/storage-parquet-protocol/pom.xml clean install -DskipTests  || exit 1
mvn clean install -DskipTests                                  2>&1 | tee testall-1.log  || exit 1
mvn test -Dhdp.version=$hdpv -fae                              2>&1 | tee testall-2.log  || exit 1
mvn -pl :kap-it pre-integration-test -Dhdp.version=$hdpv       2>&1 | tee testall-3.log  || exit 1
mvn -pl :kap-it failsafe:integration-test -Dhdp.version=$hdpv  2>&1 | tee testall-4.log  || exit 1
mvn -pl :kap-it post-integration-test -Dhdp.version=$hdpv      2>&1 | tee testall-5.log  || exit 1
mvn -pl :kap-it failsafe:verify -Dhdp.version=$hdpv            2>&1 | tee testall-6.log  || exit 1
