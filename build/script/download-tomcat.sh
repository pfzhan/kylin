#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh

rm -rf build/tomcat

tomcat_pkg_version="7.0.59"
tomcat_pkg_md5="ec570258976edf9a833cd88fd9220909"

if [ ! -f "build/apache-tomcat-${tomcat_pkg_version}.tar.gz" ]
then
    echo "no binary file found"
    wget --directory-prefix=build/ http://archive.apache.org/dist/tomcat/tomcat-7/v${tomcat_pkg_version}/bin/apache-tomcat-${tomcat_pkg_version}.tar.gz || echo "Download tomcat failed"
else
    if [ `calMd5 apache-tomcat-${tomcat_pkg_version}.tar.gz | awk '{print $1}'` != "${tomcat_pkg_md5}" ]
    then
        echo "md5 check failed"
        rm apache-tomcat-${tomcat_pkg_version}.tar.gz
        wget --directory-prefix=build/ http://archive.apache.org/dist/tomcat/tomcat-7/v${tomcat_pkg_version}/bin/apache-tomcat-${tomcat_pkg_version}.tar.gz || echo "download tomcat failed"
    fi
fi

tar -zxvf build/apache-tomcat-${tomcat_pkg_version}.tar.gz -C build/
mv build/apache-tomcat-${tomcat_pkg_version} build/tomcat
rm -rf build/tomcat/webapps/*

mv build/tomcat/conf/server.xml build/tomcat/conf/server.xml.bak
cp kylin/build/deploy/server.xml build/tomcat/conf/server.xml
echo "server.xml overwritten..."
