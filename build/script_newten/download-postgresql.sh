#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh

rm -rf build/postgresql

postgresql_version="10.7-1PGDG.rhel6.x86_64"
postgresql_server_md5="34cb34bcaf5388c96ffc758da4ed8a2a"

if [ ! -f "build/postgresql10-server-${postgresql_version}.rpm" ]
then
    echo "no binary file found "
    wget --directory-prefix=build/ https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/postgresql10-server-${postgresql_version}.rpm || echo "Download postgresSQL-server failed"
else
    if [ `calMd5 build/postgresql10-server-${postgresql_version}.rpm | awk '{print $1}'` != "${postgresql_server_md5}" ]
    then
        echo "md5 check failed"
        rm build/postgresql10-server-${postgresql_version}.rpm
        wget --directory-prefix=build/ https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/postgresql10-server-${postgresql_version}.rpm || echo "Download postgresSQL-server failed"

    fi
fi

postgresql_client_md5="0b681ba97a9ddc478f820d2287de3b2d"
if [ ! -f "build/postgresql10-${postgresql_version}.rpm" ]
then
    echo "no binary file found "
    wget --directory-prefix=build/ https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/postgresql10-${postgresql_version}.rpm || echo "Download postgresSQL-client failed"
else
    if [ `calMd5 build/postgresql10-${postgresql_version}.rpm | awk '{print $1}'` != "${postgresql_client_md5}" ]
    then
        echo "md5 check failed"
        rm build/postgresql10-${postgresql_version}.rpm
        wget --directory-prefix=build/ https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/postgresql10-${postgresql_version}.rpm || echo "Download postgresSQL-client failed"

    fi
fi

postgresql_libs_md5="28f75cae7734a0f62cb6ae774d2a1378"
if [ ! -f "build/postgresql10-libs-${postgresql_version}.rpm" ]
then
    echo "no binary file found "
    wget --directory-prefix=build/ https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/postgresql10-libs-${postgresql_version}.rpm || echo "Download postgresSQL-libs failed"
else
    if [ `calMd5 build/postgresql10-libs-${postgresql_version}.rpm | awk '{print $1}'` != "${postgresql_libs_md5}" ]
    then
        echo "md5 check failed"
        rm build/postgresql10-libs-${postgresql_version}.rpm
        wget --directory-prefix=build/ https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/postgresql10-libs-${postgresql_version}.rpm || echo "Download postgresSQL-lib failed"

    fi
fi


mkdir -p build/postgresql
cp build/postgresql10-server-${postgresql_version}.rpm build/postgresql
cp build/postgresql10-${postgresql_version}.rpm build/postgresql
cp build/postgresql10-libs-${postgresql_version}.rpm build/postgresql