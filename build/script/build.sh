#!/bin/bash

dir=$(dirname ${0})
skip_front=false
if [[ ${1} == "skip-install-front" ]]
then
    echo 'Skip install front-end dependencies'
    skip_front=true
    shift
fi

cd ${dir}/../..

echo 'Build back-end'
mvn clean install -DskipTests $@ || { exit 1; }

#package webapp
echo 'Build front-end'
cd kystudio
if [[ ${skip_front} = false ]]
then
    echo 'Install front-end dependencies'
    npm install						 || { exit 1; }
fi
npm run build		 || { exit 1; }
