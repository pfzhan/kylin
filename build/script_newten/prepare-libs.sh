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

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh
exportProjectVersions

echo "copy lib file"
rm -rf build/lib build/tool
mkdir build/lib build/tool
cp src/assembly/target/kap-assembly-${kap_version}-job.jar build/lib/newten-job.jar
cp src/spark-project/kylin-user-session-dep/target/kylin-user-session-dep-${kap_version}.jar build/lib/kylin-user-session-dep-${release_version}.jar
mkdir -p build/lib/ext
mv src/server/target/jars/kap-second-storage-clickhouse-*.jar build/lib/ext/kap-second-storage-clickhouse-${release_version}.jar


# Copied file becomes 000 for some env (e.g. Cygwin)
#chmod 644 build/lib/kylin-job-kap-${release_version}.jar
#chmod 644 build/lib/kylin-coprocessor-kap-${release_version}.jar
#chmod 644 build/lib/kylin-jdbc-kap-${release_version}.jar
#chmod 644 build/lib/kylin-storage-parquet-kap-${release_version}.jar
#chmod 644 build/tool/kylin-tool-kap-${release_version}.jar