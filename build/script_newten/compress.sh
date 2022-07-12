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

# get package name
current_branch=${branch}
if [[ "${current_branch}" = "" ]]; then
    current_branch=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p')
fi

# package as *.tar.gz
echo "package name: ${package_name}"
cd build/
rm -rf ${package_name}
mkdir ${package_name}

cp -rf CHANGELOG.md VERSION commit_SHA1 lib ${package_name}/

mkdir -p ${package_name}/lib/ext

cp -rf spark ${package_name}/
cp -rf sample_project ${package_name}/
cp -rf samples ${package_name}/
cp -rf influxdb ${package_name}/
cp -rf grafana ${package_name}/
cp -rf postgresql ${package_name}/

if [[ -d docs ]]; then
  cp -rf docs ${package_name}/
fi

# Add ssb data preparation files
mkdir -p ${package_name}/tool/ssb
cp -rf ../src/examples/sample_cube/data ${package_name}/tool/ssb/
cp -rf ../src/examples/sample_cube/create_sample_ssb_tables.sql ${package_name}/tool/ssb/

# Add ops_plan files
cp -rf ../ops_plan ${package_name}/

# Add conf profiles
mkdir -p ${package_name}/conf
mkdir -p ${package_name}/tool/conf
mkdir -p ${package_name}/server/conf
for log_conf in `find conf -name "*-log4j.xml"`; do
    cp ${log_conf} ${package_name}/${log_conf}.template
    if [[ ${log_conf} == *"tool"* ]]; then
        cp ${log_conf} ${package_name}/tool/${log_conf}
    else
        cp ${log_conf} ${package_name}/server/${log_conf}
    fi
done
cp -rf conf/kylin.properties ${package_name}/conf/kylin.properties
cp -rf conf/setenv.sh ${package_name}/conf/setenv.sh.template
cp -rf conf/query-limit-fair-scheduler.xml.template ${package_name}/conf/query-limit-fair-scheduler.xml.template
cp -rf bin/ ${package_name}/bin/
cp -rf sbin/ ${package_name}/sbin/

rm -rf ext lib commit_SHA1 VERSION # keep the spark folder on purpose

cp -rf server/webapp/dist ${package_name}/server/public
cp -rf server/newten.jar ${package_name}/server/
cp -rf server/jars ${package_name}/server/
cp -rf deploy/.keystore ${package_name}/server/
mv ${package_name}/server/jars/log4j* ${package_name}/spark/jars/
rm -rf server/

## comment all default properties, and append them to the user visible kylin.properties
## first 16 lines are license, just skip them
sed '1,21d' ../src/core-common/src/main/resources/kylin-defaults0.properties | awk '{print "#"$0}' >> ${package_name}/conf/kylin.properties

find ${package_name} -type d -exec chmod 755 {} \;
find ${package_name} -type f -exec chmod 644 {} \;
find ${package_name} -type f -name "*.sh" -exec chmod 755 {} \;
find ${package_name}/spark -type f -exec chmod 755 {} \;
find ${package_name}/influxdb -type f -exec chmod 755 {} \;
find ${package_name}/grafana -type f -exec chmod 755 {} \;
find ${package_name}/postgresql -type f -exec chmod 755 {} \;

rm -rf ../dist
mkdir -p ../dist

tar -czf ../dist/${package_name}.tar.gz ${package_name}
rm -rf ${package_name}

echo "Package ready."
cd ../dist
ls ${package_name}*.tar.gz
