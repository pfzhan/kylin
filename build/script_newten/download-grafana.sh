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

rm -rf build/grafana

grafana_version="6.2.4"
grafana_pkg_md5="b0c5b1085db30914e128980b5fe5f553"

if [ ! -f "build/grafana-${grafana_version}.linux-amd64.tar.gz" ]
then
    echo "no binary file found "
    wget --directory-prefix=build/ https://dl.grafana.com/oss/release/grafana-${grafana_version}.linux-amd64.tar.gz || echo "Download grafana failed"
else
    if [ `calMd5 build/grafana-${grafana_version}.linux-amd64.tar.gz | awk '{print $1}'` != "${grafana_pkg_md5}" ]
    then
        echo "md5 check failed"
        rm build/grafana-${grafana_version}.linux-amd64.tar.gz
        wget --directory-prefix=build/ https://dl.grafana.com/oss/release/grafana-${grafana_version}.linux-amd64.tar.gz || echo "Download grafana failed"
    fi
fi

tar -zxf build/grafana-${grafana_version}.linux-amd64.tar.gz -C build/ || { exit 1; }
mv build/grafana-${grafana_version} build/grafana

test -d 'build/grafana/conf/provisioning' && rm -rf build/grafana/conf/provisioning

test -d 'build/grafana/public/test' && rm -rf build/grafana/public/test

cp -rf build/deploy/grafana/dashboards build/grafana/
cp -rf build/deploy/grafana/provisioning build/grafana/conf/
cp -rf build/deploy/grafana/custom.ini build/grafana/conf/
