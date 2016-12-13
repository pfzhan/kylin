#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

# get package name
current_branch=${branch}
if [ "${current_branch}" = "" ]; then
    current_branch=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p')
fi
target_env="hbase0.98"
feature="-plus"
if [[ "${current_branch}" =~ "cdh" ]]; then
    target_env="cdh5.7"
elif [[ "${current_branch}" =~ "hbase102" ]]; then
    target_env="hbase102"
elif [[ "${current_branch}" =~ "hbase1.x" ]]; then
    target_env="hbase1.x"
fi
if [ "${PACKAGE_PLUS}" = "0" ]; then
    feature=""
fi
package_name=kap${feature}-${release_version}-${target_env}

# package as *.tar.gz
echo "package name: ${package_name}"
cd build/
rm -rf ${package_name}
mkdir ${package_name}

# add apache kylin files
cp -rf ../kylin/build/bin ../kylin/build/conf ../kylin/examples/sample_cube ${package_name}
if [ -f "kybot/diag.sh" ]; then
    mv kybot/diag.sh ${package_name}/bin
fi
cp -rf commit_SHA1 lib kybot tomcat spark ${package_name}/

# Add min/prod profiles
cp -rf conf/profile_min ${package_name}/conf
rm -rf ${package_name}/conf/profile_min/kap-plus.properties
cp -rf conf/profile_prod ${package_name}/conf
rm -rf ${package_name}/conf/profile_prod/kap-plus.properties

cp -rf conf/kylin-tools-log4j.properties ${package_name}/conf/
cp -rf conf/kylin-server-log4j.properties ${package_name}/conf/
cp -rf conf/spark-driver-log4j.properties ${package_name}/conf/
cp -rf conf/spark-executor-log4j.properties ${package_name}/conf/

cp -rf conf/userctrl.acl ${package_name}/conf/
cp -rf bin/* ${package_name}/bin/

# update kap plus config files
if [ "${PACKAGE_PLUS}" != "0" ]; then
    cat conf/profile_min/kap-plus.properties >> ${package_name}/conf/profile_min/kylin.properties
    cat conf/profile_prod/kap-plus.properties >> ${package_name}/conf/profile_prod/kylin.properties
fi

# update symblink, use minimum profile as default
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

rm -rf lib tomcat commit_SHA1 # keep the spark folder on purpose
find ${package_name} -type d -exec chmod 755 {} \;
find ${package_name} -type f -exec chmod 644 {} \;
find ${package_name} -type f -name "*.sh" -exec chmod 755 {} \;
find ${package_name}/spark -type f -exec chmod 755 {} \;

rm -rf ../dist
mkdir -p ../dist
tar -cvzf ../dist/${package_name}-orig.tar.gz ${package_name}
rm -rf ${package_name}

cd ../dist
# package obf tar
if [ "$SKIP_OBF" != "1" ]; then
    tar -xzf ${package_name}-orig.tar.gz

    mv ../tmp/kylin.war ${package_name}/tomcat/webapps/kylin.war
    mv ../tmp/kylin-coprocessor-kap-${release_version}-obf.jar ${package_name}/lib/kylin-coprocessor-kap-${release_version}.jar
    mv ../tmp/kylin-storage-parquet-kap-${release_version}-obf.jar ${package_name}/lib/kylin-storage-parquet-kap-${release_version}.jar
    mv ../tmp/kylin-job-kap-${release_version}-obf.jar ${package_name}/lib/kylin-job-kap-${release_version}.jar
    mv ../tmp/kylin-tool-kap-${release_version}-obf.jar ${package_name}/lib/kylin-tool-kap-${release_version}.jar
    tar -cvzf ${package_name}.tar.gz ${package_name}

    rm -r ../tmp
    rm -rf ${package_name}

    mv ../server_mapping.txt ${package_name}-obf.mapping
fi

echo "Package ready."
ls ${package_name}*.tar.gz