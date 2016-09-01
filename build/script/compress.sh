#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

# Package as *.tar.gz
echo 'package tar.gz'
package_name=kylin-kap-${release_version}-bin
cd build/
rm -rf ${package_name}
mkdir ${package_name}

# add apache kylin files
cp -rf ../kylin/build/bin ../kylin/build/conf ../kylin/examples/sample_cube ${package_name}
if [ -f "kybot/diag.sh" ]; then
    mv kybot/diag.sh ${package_name}/bin
fi
cp -rf commit_SHA1 lib kybot tomcat spark ${package_name}/

# add kap files
cp -rf conf/kylin.properties ${package_name}/conf/
cp -rf bin/* ${package_name}/bin/

# update kap plus config files
if [ "${PACKAGE_PLUS}" == "1" ]; then
    cat conf/plus/kylin.properties.append >> ${package_name}/conf/kylin.properties
fi

rm -rf lib tomcat commit_SHA1 # keep the spark folder on purpose
find ${package_name} -type d -exec chmod 755 {} \;
find ${package_name} -type f -exec chmod 644 {} \;
find ${package_name} -type f -name "*.sh" -exec chmod 755 {} \;
find ${package_name}/spark -type f -exec chmod 755 {} \;

rm -rf ../dist
mkdir -p ../dist
tar -cvzf ../dist/${package_name}.tar.gz ${package_name}
rm -rf ${package_name}

cd ../dist
# package obf tar
if [ "$SKIP_OBF" != "1" ]; then
    tar -xzf ${package_name}.tar.gz

    mv ../tmp/kylin.war ${package_name}/tomcat/webapps/kylin.war
    mv ../tmp/kylin-coprocessor-kap-${release_version}-obf.jar ${package_name}/lib/kylin-coprocessor-kap-${release_version}.jar
    mv ../tmp/kylin-storage-parquet-kap-${release_version}-obf.jar ${package_name}/lib/kylin-storage-parquet-kap-${release_version}.jar
    mv ../tmp/kylin-job-kap-${release_version}-obf.jar ${package_name}/lib/kylin-job-kap-${release_version}.jar
    mv ../tmp/kylin-tool-kap-${release_version}-obf.jar ${package_name}/lib/kylin-tool-kap-${release_version}.jar
    tar -cvzf ${package_name}-obf.tar.gz ${package_name}

    rm -r ../tmp
    rm -rf ${package_name}

    mv ../server_mapping.txt ${package_name}-obf.mapping
fi

echo "Package ready."
ls ${package_name}*.tar.gz
