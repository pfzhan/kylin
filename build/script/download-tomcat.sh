#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh

rm -rf build/tomcat

tomcat_pkg_version="7.0.82"
tomcat_pkg_md5="b9c07fb4f37063e9e8185972b3f88a98"

if [ ! -f "build/apache-tomcat-${tomcat_pkg_version}.tar.gz" ]
then
    echo "no binary file found"
    wget --directory-prefix=build/ http://archive.apache.org/dist/tomcat/tomcat-7/v${tomcat_pkg_version}/bin/apache-tomcat-${tomcat_pkg_version}.tar.gz || echo "Download tomcat failed"
else
    if [ `calMd5 build/apache-tomcat-${tomcat_pkg_version}.tar.gz | awk '{print $1}'` != "${tomcat_pkg_md5}" ]
    then
        echo "md5 check failed"
        rm build/apache-tomcat-${tomcat_pkg_version}.tar.gz
        wget --directory-prefix=build/ http://archive.apache.org/dist/tomcat/tomcat-7/v${tomcat_pkg_version}/bin/apache-tomcat-${tomcat_pkg_version}.tar.gz || echo "download tomcat failed"
    fi
fi

tar -zxvf build/apache-tomcat-${tomcat_pkg_version}.tar.gz -C build/   || { exit 1; }
mv build/apache-tomcat-${tomcat_pkg_version} build/tomcat
rm -rf build/tomcat/webapps/*

mv build/tomcat/conf/server.xml build/tomcat/conf/server.xml.bak
cp build/deploy/server.xml build/tomcat/conf/server.xml
cp build/deploy/server.xml build/tomcat/conf/server.xml.init
echo "server.xml overwritten..."


cp build/tomcat/conf/catalina.properties build/tomcat/conf/catalina.properties.bak
sed -i "s/org\.apache\.catalina\.startup\.ContextConfig\.jarsToSkip=.*/org\.apache\.catalina\.startup\.ContextConfig\.jarsToSkip=*.jar/g" build/tomcat/conf/catalina.properties
sed -i "s/org\.apache\.catalina\.startup\.TldConfig\.jarsToSkip=.*/org\.apache\.catalina\.startup\.TldConfig\.jarsToSkip=*.jar/g" build/tomcat/conf/catalina.properties
echo "catalina.properties overwritten..."


cp build/deploy/.keystore build/tomcat/conf/.keystore

cp extensions/tomcat-ext/target/kap-tomcat-ext-${kap_version}.jar build/tomcat/lib/kap-tomcat-ext-${kap_version}.jar
chmod 644 build/tomcat/lib/kap-tomcat-ext-${kap_version}.jar

# add ROOT application
mkdir -p build/tomcat/webapps/ROOT
cat > build/tomcat/webapps/ROOT/index.html <<EOL
<html>
  <head>
    <meta http-equiv="refresh" content="1;url=kylin">
  </head>
</html>
EOL

