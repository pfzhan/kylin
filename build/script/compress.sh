#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

# Package as *.tar.gz
echo 'package tar.gz'
package_name=kylin-kap-${kap_version}-bin
cd build/
rm -rf ${package_name}
mkdir ${package_name}

cp -rf ../kylin/build/bin ../kylin/build/conf ../kylin/examples/sample_cube ${package_name}
cp -rf commit_SHA1 lib tomcat ${package_name}/
cp -rf conf/* ${package_name}/conf/

rm -rf lib tomcat commit_SHA1
find ${package_name} -type d -exec chmod 755 {} \;
find ${package_name} -type f -exec chmod 644 {} \;
find ${package_name} -type f -name "*.sh" -exec chmod 755 {} \;
mkdir -p ../dist
tar -cvzf ../dist/${package_name}.tar.gz ${package_name}
rm -rf ${package_name}

echo "Package ready: dist/${package_name}.tar.gz"
