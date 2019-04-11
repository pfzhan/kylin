#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh
exportProjectVersions

# get package name
current_branch=${branch}
if [ "${current_branch}" = "" ]; then
    current_branch=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p')
fi

timestamp=`date '+%Y%m%d%H%M%S'`
package_name="Kyligence-Enterprise-${release_version}.${timestamp}"
# package as *.tar.gz
echo "package name: ${package_name}"
cd build/
rm -rf ${package_name}
mkdir ${package_name}

cp -rf CHANGELOG.md VERSION commit_SHA1 lib tool LICENSE ${package_name}/

if [ "${PACKAGE_SPARK}" = "1" ]; then
    cp -rf spark ${package_name}/
fi

cp -rf influxdb ${package_name}/

# Add ssb data preparation files
mkdir -p ${package_name}/tool/ssb
cp -rf ../src/examples/sample_cube/data ${package_name}/tool/ssb/
cp -rf ../src/examples/sample_cube/create_sample_ssb_tables.sql ${package_name}/tool/ssb/

# Add min/prod profiles
mkdir ${package_name}/conf

cp -rf conf/profile_min ${package_name}/conf/
cp -rf conf/profile_prod ${package_name}/conf/
cp -rf conf/kylin-tools-log4j.properties ${package_name}/conf/
cp -rf conf/kylin-server-log4j.properties ${package_name}/conf/
cp -rf conf/spark-driver-log4j.properties ${package_name}/conf/
cp -rf conf/spark-executor-log4j.properties ${package_name}/conf/
cp -rf conf/fairscheduler.xml ${package_name}/conf/
cp -rf conf/setenv.sh ${package_name}/conf/
cp -rf bin/ ${package_name}/bin/

# update symblink, use production profile as default
ln -sfn profile_min profile
mv profile ${package_name}/conf/
ln -sfn profile/kylin.properties kylin.properties
mv kylin.properties ${package_name}/conf/

rm -rf ext lib tomcat commit_SHA1 VERSION # keep the spark folder on purpose

mkdir ${package_name}/server
cp -rf server/webapp/dist ${package_name}/server/public
cp -rf server/newten.jar ${package_name}/server/
cp -rf server/jars ${package_name}/server/
rm -rf server/

#add udf jar to lib
cp ../src/udf/target/kap-udf-${kap_version}.jar ${package_name}/lib/kylin-udf-${release_version}.jar

## comment all default properties, and append them to the user visible kylin.properties
## first 16 lines are license, just skip them
sed '1,21d' ../src/core-common/src/main/resources/kylin-defaults0.properties | awk '{print "#"$0}' >> ${package_name}/conf/profile_min/kylin.properties
sed '1,21d' ../src/core-common/src/main/resources/kylin-defaults0.properties | awk '{print "#"$0}' >> ${package_name}/conf/profile_prod/kylin.properties

find ${package_name} -type d -exec chmod 755 {} \;
find ${package_name} -type f -exec chmod 644 {} \;
find ${package_name} -type f -name "*.sh" -exec chmod 755 {} \;
find ${package_name}/spark -type f -exec chmod 755 {} \;
find ${package_name}/influxdb -type f -exec chmod 755 {} \;

rm -rf ../dist
mkdir -p ../dist
tar -cvzf ../dist/${package_name}.tar.gz ${package_name}
rm -rf ${package_name}

cd ../dist

# package obf tar
if [ "$SKIP_OBF" != "1" ]; then
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
