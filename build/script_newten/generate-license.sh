#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..


source build/script_newten/functions.sh
exportProjectVersions
pwd
date=`date "+%Y-%m-%d"`
exp_date=`date -d "+1 month" +%Y-%m-%d`
dates=${date},${exp_date}
license_tmp1=$(echo -n ${dates}|md5sum|cut -d ' ' -f1)
license_tmp2=${dates}${license_tmp1}
license=$(echo -n ${license_tmp2}|md5sum|cut -d ' ' -f1)
cat << EOF > build/LICENSE
${dates}
${license}
EOF