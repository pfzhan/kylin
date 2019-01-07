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
feature="-0.0.1"

package_name=newten${feature}-${release_version}
# package as *.tar.gz
echo "package name: ${package_name}"
cd build/
rm -rf ${package_name}
mkdir ${package_name}

cp -r ../src/examples/sample_cube ${package_name}

cp -rf CHANGELOG.md VERSION commit_SHA1 ext lib tool ${package_name}/

if [ "${PACKAGE_SPARK}" = "1" ]; then
    cp -rf spark ${package_name}/
fi

cp -rf influxdb ${package_name}/

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
cp -rf ssb-kylin/ ${package_name}/ssb-kylin/

cp -rf ../src/examples ${package_name}/

# update symblink, use production profile as default
ln -sfn profile_min profile
mv profile ${package_name}/conf/
ln -sfn profile/kylin.properties kylin.properties
mv kylin.properties ${package_name}/conf/
ln -sfn profile/kylin_hive_conf.xml kylin_hive_conf.xml
mv kylin_hive_conf.xml ${package_name}/conf/
ln -sfn profile/kylin_job_conf.xml kylin_job_conf.xml
mv kylin_job_conf.xml ${package_name}/conf/
ln -sfn profile/kylin_job_conf_inmem.xml kylin_job_conf_inmem.xml
mv kylin_job_conf_inmem.xml ${package_name}/conf/
ln -sfn profile/kylin-kafka-consumer.xml kylin-kafka-consumer.xml
mv kylin-kafka-consumer.xml ${package_name}/conf/

rm -rf ext lib tomcat commit_SHA1 VERSION # keep the spark folder on purpose

#add sparder jar  ensure the jersey 2.x can be load first
cp spark/jars/javax.ws.rs-api-*.jar ${package_name}/ext/
cp spark/jars/jersey-server-*.jar ${package_name}/ext/

mkdir ${package_name}/server
cp -rf server/webapp/dist ${package_name}/server/public
cp -rf server/newten.jar ${package_name}/server/
rm -rf server/

#add udf jar to lib
cp ../src/udf/target/kap-udf-${release_version}.jar ${package_name}/lib/kylin-udf-${release_version}.jar

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
tar -cvzf ../dist/${package_name}-orig.tar.gz ${package_name}
rm -rf ${package_name}

cd ../dist

echo "Package ready."
ls ${package_name}*.tar.gz
