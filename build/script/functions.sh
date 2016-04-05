#!/usr/bin/bash

function checkCommandExits() {
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
        export kap_version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -Ev '(^\[|Download\w+:)'`
        echo "KAP Version: ${kap_version}"
    fi
    if [ -z "${kylin_versoin}" ]; then
        export kylin_version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -f kylin | grep -Ev '(^\[|Download\w+:)'`
        echo "Apache Kylin Version: ${kylin_version}"
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