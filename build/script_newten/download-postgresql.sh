#!/bin/bash

##
## Copyright (C) 2020 Kyligence Inc. All rights reserved.
##
## http://kyligence.io
##
## This software is the confidential and proprietary information of
## Kyligence Inc. ("Confidential Information"). You shall not disclose
## such Confidential Information and shall use it only in accordance
## with the terms of the license agreement you entered into with
## Kyligence Inc.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
## "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
## LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
## A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
## OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
## SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
## LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
## DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
## THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
## (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
## OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
##

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh

mkdir -p build/postgresql


pg_urls=(
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/postgresql10-libs-10.7-1PGDG.rhel6.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/postgresql10-10.7-1PGDG.rhel6.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/postgresql10-server-10.7-1PGDG.rhel6.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-7-x86_64/postgresql10-libs-10.7-1PGDG.rhel7.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-7-x86_64/postgresql10-10.7-1PGDG.rhel7.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-7-x86_64/postgresql10-server-10.7-1PGDG.rhel7.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-8-x86_64/postgresql10-libs-10.8-1PGDG.rhel8.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-8-x86_64/postgresql10-10.8-1PGDG.rhel8.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-8-x86_64/postgresql10-server-10.8-1PGDG.rhel8.x86_64.rpm"
)

pg_file_md5=(
    "28f75cae7734a0f62cb6ae774d2a1378"
    "0b681ba97a9ddc478f820d2287de3b2d"
    "34cb34bcaf5388c96ffc758da4ed8a2a"
    "648a350f2421a8320cebe40a812909e9"
    "62a2915053f5f2ffd5ef1bbbc106e7be"
    "5ef931ddedeca1c81a998763292aafad"
    "23cea91ae23d4f30802d2d3f5b47eac4"
    "dae27bb23602348d593a731968f23331"
    "a5f563d7bf7fd0444250bb4bba6f3d4d"
)

for ((i=0;i<${#pg_urls[@]};i++))
do
    url=${pg_urls[$i]}
    file_name=${url##*/}
    if [ ! -f "build/postgresql/$file_name" ]
    then
        echo "No binary file found "
        wget --directory-prefix=build/postgresql/ $url || echo "Download $file_name failed."
    else
        if [ `calMd5 build/postgresql/$file_name | awk '{print $1}'` != "${pg_file_md5[$i]}" ]
        then
            echo "md5 check failed"
            rm build/$file_name
            wget --directory-prefix=build/postgresql/ $url || { echo "Download $file_name failed." && exit 1; }
        fi
    fi
done



cp build/deploy/licenses/postgresql-license build/postgresql/LICENSE || { echo "No license for PostgreSQL found, please check build/deploy/licenses" && exit 1; }
