#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

echo 'Build back-end'
mvn clean install -DskipTests                                                       || { exit 1; }

#package webapp
echo 'Build front-end'
cd webapp
npm install -g bower			 || { exit 1; }
bower --allow-root install		 || { exit 1; }
npm install						 || { exit 1; }
npm install -g grunt-cli		 || { exit 1; }
grunt dev --buildEnv=dev --buildNumber=`date "+%Y%m%d%H%M%S"`		 || { exit 1; }
