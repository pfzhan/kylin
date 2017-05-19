#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

echo 'Build back-end'
mvn clean install -DskipTests $@ || { exit 1; }

#package webapp
echo 'Build front-end'
cd kystudio
npm install						 || { exit 1; }
npm run build		 || { exit 1; }
