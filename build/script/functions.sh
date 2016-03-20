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
        export kap_version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\['`
        echo "KAP Version: ${kap_version}"
    fi
    if [ -z "${kylin_versoin}" ]; then
        export kylin_version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -f kylin| grep -v '\['`
        echo "Apache Kylin Version: ${kylin_version}"
    fi
}