#!/bin/bash

if [[ $# -ne 3 ]]; then
    echo "Usage: copy-release.sh [build num for hbase0.98] [build num for hbase1.x] [build num for cdh5.7]"
    exit 1
fi

# configurations
remote_server=10.1.1.28
remote_user=root
remote_path=/root/kap/release
kap_version=2.1

# parameters
hbase_98_build=$1
hbase_1x_build=$2
hbase_cdh57_build=$3

# copy package for hbase 0.98
echo "copying package for hbase 0.8 from ${remote_user}@${remote_server} - version: ${kap_version} build: ${hbase_98_build}"
scp ${remote_user}@${remote_server}:${remote_path}/${kap_version}-hbase0.98/kap-${kap_version}-hbase0.98-${hbase_98_build}/kap-*.tar.gz .
scp ${remote_user}@${remote_server}:${remote_path}/${kap_version}-hbase0.98/kap-plus-${kap_version}-hbase0.98-${hbase_98_build}/kap-*.tar.gz .

# copy package for hbase 1.x
echo "copying package for hbase 1.x from ${remote_user}@${remote_server} - version: ${kap_version} build: ${hbase_1x_build}"
scp ${remote_user}@${remote_server}:${remote_path}/${kap_version}-hbase1.x/kap-${kap_version}-hbase1.x-${hbase_1x_build}/kap-*.tar.gz .
scp ${remote_user}@${remote_server}:${remote_path}/${kap_version}-hbase1.x/kap-plus-${kap_version}-hbase1.x-${hbase_1x_build}/kap-*.tar.gz .

# copy package for cdh5.7
echo "copying package for cdh5.7 from ${remote_user}@${remote_server} - version: ${kap_version} build: ${hbase_cdh57_build}"
scp ${remote_user}@${remote_server}:${remote_path}/${kap_version}-cdh5.7/kap-${kap_version}-cdh5.7-${hbase_cdh57_build}/kap-*.tar.gz .
scp ${remote_user}@${remote_server}:${remote_path}/${kap_version}-cdh5.7/kap-plus-${kap_version}-cdh5.7-${hbase_cdh57_build}/kap-*.tar.gz .