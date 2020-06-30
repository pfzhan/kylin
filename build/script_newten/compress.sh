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

rm -rf lib/kylin-user-session-dep-${release_version}.jar
cp -rf CHANGELOG.md VERSION commit_SHA1 lib tool LICENSE ${package_name}/

mkdir ${package_name}/lib/ext

if [[ "${PACKAGE_SPARK}" = "1" ]]; then
    cp -rf spark ${package_name}/
fi

cp -rf samples ${package_name}/
cp -rf influxdb ${package_name}/
cp -rf grafana ${package_name}/
cp -rf postgresql ${package_name}/

# Add ssb data preparation files
mkdir -p ${package_name}/tool/ssb
cp -rf ../src/examples/sample_cube/data ${package_name}/tool/ssb/
cp -rf ../src/examples/sample_cube/create_sample_ssb_tables.sql ${package_name}/tool/ssb/

cp -rf deploy/grafana/dashboards ${package_name}/grafana/
cp -rf deploy/grafana/provisioning ${package_name}/grafana/conf/
cp -rf deploy/grafana/custom.ini ${package_name}/grafana/conf/

# Add conf profiles
mkdir -p ${package_name}/conf
mkdir -p ${package_name}/tool/conf
mkdir -p ${package_name}/server/conf
for log_conf in `find conf -name "*-log4j.properties"`; do
    cp ${log_conf} ${package_name}/${log_conf}.template
    if [[ ${log_conf} == *"tool"* ]]; then
        cp ${log_conf} ${package_name}/tool/${log_conf}
    else
        cp ${log_conf} ${package_name}/server/${log_conf}
    fi
done
cp -rf conf/kylin.properties ${package_name}/conf/kylin.properties
cp -rf conf/setenv.sh ${package_name}/conf/setenv.sh.template
cp -rf bin/ ${package_name}/bin/
cp -rf sbin/ ${package_name}/sbin/

rm -rf ext lib tomcat commit_SHA1 VERSION # keep the spark folder on purpose

mkdir ${package_name}/server
cp -rf server/webapp/dist ${package_name}/server/public
cp -rf server/newten.jar ${package_name}/server/
cp -rf server/jars ${package_name}/server/
cp -rf deploy/.keystore ${package_name}/server/
rm -rf server/

#add udf jar to lib
cp ../src/udf/target/kap-udf-${kap_version}.jar ${package_name}/lib/kylin-udf-${release_version}.jar


# add kylin user jar to lib
rm -rf ../tmp/merge
mkdir -p ../tmp/merge
cd ../tmp/merge
jar -xf ../kylin-user-session-dep-${release_version}-obf.jar
jar -xf ../../src/spark-project/kylin-user-session/target/original-kylin-user-session-${kap_version}.jar
jar -cfM kylin-user-session-${release_version}.jar  .
cd ../../build
mv ../tmp/merge/kylin-user-session-${release_version}.jar  ${package_name}/lib/kylin-user-session-${release_version}.jar

# add hadoop3 jar to spark
cp -rf hadoop3 ${package_name}/spark


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
tar -cvzf ../dist/${package_name}.tar.gz ${package_name}
rm -rf ${package_name}

cd ../dist

# package obf tar
if [[ "$SKIP_OBF" != "1" ]]; then
    tar -xzf ${package_name}.tar.gz

    mv ../tmp/kap-assembly-${release_version}-job-obf.jar ${package_name}/lib/newten-job.jar
    mv ../tmp/kap-tool-assembly-${release_version}-assembly-obf.jar ${package_name}/tool/kap-tool-${release_version}.jar
    tar -cvzf ${package_name}.tar.gz ${package_name}

    rm -r ../tmp
    rm -rf ${package_name}

    mv ../server_mapping.txt ${package_name}-obf.mapping
fi

echo "Package ready."
ls ${package_name}*.tar.gz
