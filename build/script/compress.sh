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
elif [[ "${current_branch}" =~ "hbase" ]]; then
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

# rename kylin files to min
mv ${package_name}/conf/kylin.properties ${package_name}/conf/kylin.min.properties
mv ${package_name}/conf/kylin_hive_conf.xml ${package_name}/conf/kylin_hive_conf.min.xml
mv ${package_name}/conf/kylin_job_conf.xml ${package_name}/conf/kylin_job_conf.min.xml
mv ${package_name}/conf/kylin_job_conf_inmem.xml ${package_name}/conf/kylin_job_conf_inmem.min.xml

# add kap prod files
cp -rf conf/kylin.min.properties ${package_name}/conf/
cp -rf conf/kylin.prod.properties ${package_name}/conf/
cp -rf conf/kylin_hive_conf.prod.xml ${package_name}/conf/
cp -rf conf/kylin_job_conf.prod.xml ${package_name}/conf/
cp -rf conf/kylin_job_conf_inmem.prod.xml ${package_name}/conf/

cp -rf conf/kylin-tools-log4j.properties ${package_name}/conf/
cp -rf conf/kylin-server-log4j.properties ${package_name}/conf/
cp -rf conf/userctrl.acl ${package_name}/conf/
cp -rf bin/* ${package_name}/bin/

# update kap plus config files
if [ "${PACKAGE_PLUS}" != "0" ]; then
    cat conf/plus/kap-plus.min.properties >> ${package_name}/conf/kylin.min.properties
    cat conf/plus/kap-plus.prod.properties >> ${package_name}/conf/kylin.prod.properties
fi

# update symblink
ln -sf kylin.min.properties kylin.properties
mv kylin.properties ${package_name}/conf/
ln -sf kylin_hive_conf.min.xml kylin_hive_conf.xml
mv kylin_hive_conf.xml ${package_name}/conf/
ln -sf kylin_job_conf.min.xml kylin_job_conf.xml
mv kylin_job_conf.xml ${package_name}/conf/
ln -sf kylin_job_conf_inmem.min.xml kylin_job_conf_inmem.xml
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
