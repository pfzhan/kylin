#!/bin/bash

dir=$(dirname ${0})

cd ${dir}/../..

echo 'Build back-end'
mvn clean install -DskipTests $@ || { exit 1; }

#package webapp
echo 'Build front-end'
if [ "${SKIP_FRONT}" = "0" ];
then
    cd kystudio
    echo 'Install front-end dependencies'
    if ! [[ -x "$(command -v cnpm)" ]]; then
        npm install -g cnpm --registry=https://registry.npm.taobao.org  || { exit 1; }
    fi
    cnpm install						 || { exit 1; }
    npm run build		 || { exit 1; }
fi

