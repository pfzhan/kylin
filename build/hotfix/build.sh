#!/usr/bin/env bash

dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
if [[ $# -ne 2 ]]; then
    echo "Usage: build.sh <tar package path> <base commit id>"
    exit 1
fi

package_tar=$1
base_commit=$2

working_dir=temp_hotfix
mkdir $working_dir
tar -zxvf ${package_tar} -C ${working_dir}

cp ${dir}/install.sh ${working_dir}

cd ${working_dir}
kap_dir=`ls|grep "kap-*"`
mv ${kap_dir} lib
echo ${kap_dir} > name
echo ${base_commit}@KAP > base_commit
cd lib

# remove spark files
rm -rf spark
# remove tomcat files
cd tomcat
rm -rf bin lib temp work LICENSE RUNNING.txt RELEASE-NOTES NOTICE conf logs
cd ../../../

# add base conf and bin
mkdir ${working_dir}/base
backup_commit=`git rev-parse HEAD`
git checkout $base_commit
cp -r ${dir}/../bin ${working_dir}/base/
cp -r ${dir}/../conf ${working_dir}/base/
if [[ ${kap_dir} =~ "plus" ]]; then
    cat ${working_dir}/base/conf/plus/kap-plus.min.properties >> ${working_dir}/base/conf/kap-plus.min.properties
    cat ${working_dir}/base/conf/plus/kap-plus.prod.properties >> ${working_dir}/base/conf/kap-plus.prod.properties
fi
rm -rf ${working_dir}/base/conf/plus
git checkout $backup_commit

mv ${working_dir} ${kap_dir}-hotfix
mkdir -p ${dir}/../../dist
tar -zcvf ${dir}/../../dist/${kap_dir}-hotfix.tar.gz ${kap_dir}-hotfix
rm -rf ${kap_dir}-hotfix

echo "Please find the hotfix package at: "
ls ${dir}/../../dist/${kap_dir}-hotfix.tar.gz