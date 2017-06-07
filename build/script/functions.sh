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
        export kap_version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -E '^[0-9]+\.[0-9]+\.[0-9]+' `
        echo "KAP Version: ${kap_version}"
    fi
    if [ -z "${kylin_versoin}" ]; then
        export kylin_version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -f kylin | grep -E '^[0-9]+\.[0-9]+\.[0-9]+' `
        echo "Apache Kylin Version: ${kylin_version}"
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

function removeKAPPlusConfigs() {
    file=$1
    startline=`cat -n ${file} | sed -n '/==========KAP PLUS ONLY START==========/p' | awk '{print $1}'`
    endline=`cat -n ${file} | sed -n '/==========KAP PLUS ONLY END==========/p' | awk '{print $1}'`
    sed -i.plusbak "${startline},${endline}d" ${file}
}

function restoreKAPPlusConfigs() { 
    file=$1
    if [ -f ${file}.plusbak ]; then
        mv ${file}.plusbak ${file}
    fi
}
