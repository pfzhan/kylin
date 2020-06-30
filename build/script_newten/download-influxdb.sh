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

rm -rf build/influxdb

influxdb_version="1.6.4.x86_64"
influxdb_pkg_md5="e24a00fb3b41d2974f85def72035f9f2"

if [ ! -f "build/influxdb-${influxdb_version}.rpm" ]
then
    echo "No binary file found."
    wget --directory-prefix=build/ https://repos.influxdata.com/centos/6/x86_64/stable/influxdb-${influxdb_version}.rpm || echo "Download InfluxDB failed."
else
    if [ `calMd5 build/influxdb-${influxdb_version}.rpm | awk '{print $1}'` != "${influxdb_pkg_md5}" ]
    then
        echo "md5 check failed."
        rm build/influxdb-${influxdb_version}.rpm
        wget --directory-prefix=build/ https://repos.influxdata.com/centos/6/x86_64/stable/influxdb-${influxdb_version}.rpm || echo "Download InfluxDB failed."

    fi
fi

mkdir -p build/influxdb
cp build/influxdb-${influxdb_version}.rpm build/influxdb

cp build/deploy/licenses/influxdb-license build/influxdb/LICENSE || { echo "No license for InfluxDB found, please check build/deploy/licenses" && exit 1; }
