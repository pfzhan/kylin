#!/usr/bin/bash

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

function checkCommandExists() {
    echo "Checking ${1}..."
    if [ -z "$(command -v ${1})" ]
    then
        echo "Please install ${1} first so that Kylin packaging can proceed"
        exit 1
    else
        echo "${1} check passed"
    fi
}

function exportProjectVersions() {
    if [ -z "${kap_version}" ]; then
        export kap_version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -E '^[0-9]+\.[0-9]+\.[0-9]+' `
        echo "KAP Version: ${kap_version}"
    fi
    if [ -z "${release_version}" ]; then
        export release_version=$kap_version
    fi
}

function detectOSType() {
    OS_TYPE="linux"
    if [[ `uname -a` =~ "Darwin" ]]; then
        OS_TYPE="mac"
    elif [[ `uname -a` =~ "Cygwin" ]]; then
        OS_TYPE="windows"
    fi
    echo $OS_TYPE
}

function calMd5() {
    OS_TYPE=`detectOSType`
    if [[ "$OS_TYPE" == "mac" ]]; then
        md5 -q $1
    elif [[ "$OS_TYPE" == "windows" ]]; then
        md5sum $1
    else
        md5sum $1
    fi
}

function checkDownloadSparkVersion() {
    if [[ -z $1 ]];
    then
        echo "spark version check failed, download spark version parameter is null"
        exit 1
    fi

    download_spark_release_version=$1
    spark_version_pom=`mvn help:evaluate -Dexpression=spark.version | grep -E '^[0-9]+\.[0-9]+\.[0-9]+' `
    pom_spark_release_version=spark-newten-"`echo ${spark_version_pom}| sed "s/-kylin//g"`"

    if [[ $download_spark_release_version != $pom_spark_release_version ]];
    then
        echo "spark version check failed, download version is ${download_spark_release_version}, but ${pom_spark_release_version} in pom file"
        exit 1
    fi
}
