#!/bin/bash

dir=$(dirname ${0})

cd ${dir}/../..

echo 'Build back-end'
mvn clean install -DskipTests $@ || { exit 1; }

#package webapp
echo 'Build front-end'
cd kystudio
if [ "${SKIP_FRONT}" = "0" ];
then
    echo 'Install front-end dependencies'
    npm install						 || { exit 1; }
fi

npm run build		 || { exit 1; }
