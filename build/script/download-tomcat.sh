#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh

rm -rf build/tomcat

tomcat_pkg_version="8.5.9"
tomcat_pkg_md5="b41270a64b7774c964e4bec813eea2ed"

if [ ! -f "build/apache-tomcat-${tomcat_pkg_version}.tar.gz" ]
then
    echo "no binary file found"
    wget --directory-prefix=build/ http://archive.apache.org/dist/tomcat/tomcat-8/v${tomcat_pkg_version}/bin/apache-tomcat-${tomcat_pkg_version}.tar.gz || echo "Download tomcat failed"
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
echo "server.xml overwritten..."

mv build/tomcat/conf/context.xml build/tomcat/conf/context.xml.bak
cp build/deploy/context.xml build/tomcat/conf/context.xml
echo "context.xml overwritten..."

cp build/deploy/.keystore build/tomcat/conf/.keystore

cp kylin/tomcat-ext/target/kylin-tomcat-ext-${kylin_version}.jar build/tomcat/lib/kylin-tomcat-ext-${kylin_version}.jar
chmod 644 build/tomcat/lib/kylin-tomcat-ext-${kylin_version}.jar


