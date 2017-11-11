#!/bin/bash

dir=$(dirname ${0})
install_front=false
if [[ ${1} == "skip-install-front" ]]
then
    echo 'Skip install front-end dependencies'
    install_front=true
    shift
fi

cd ${dir}/../..

echo 'Build back-end'
mvn clean install -DskipTests $@ || { exit 1; }

#package webapp
echo 'Build front-end'
cd kystudio
if [[ ${install_front} = false ]]
then
    echo 'Install front-end dependencies'
    npm install						 || { exit 1; }
fi
npm run build		 || { exit 1; }
