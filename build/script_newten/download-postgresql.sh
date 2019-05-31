#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh

rm -rf build/postgresql

postgresql_version="10.7-1PGDG.rhel6.x86_64"
ppostgresql_pkg_md5="34cb34bcaf5388c96ffc758da4ed8a2a"

if [ ! -f "build/postgresql10-server-${postgresql_version}.rpm" ]
then
    echo "no binary file found "
    wget --directory-prefix=build/ https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/postgresql10-server-${postgresql_version}.rpm || echo "Download postgresSQL failed"
else
    if [ `calMd5 build/postgresql10-server-${postgresql_version}.rpm | awk '{print $1}'` != "${postgresql_pkg_md5}" ]
    then
        echo "md5 check failed"
        rm build/postgresql10-server-${postgresql_version}.rpm
        wget --directory-prefix=build/ https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/postgresql10-server-${postgresql_version}.rpm || echo "Download postgresSQL failed"

    fi
fi

mkdir -p build/postgresql
cp build/postgresql10-server-${postgresql_version}.rpm build/postgresql