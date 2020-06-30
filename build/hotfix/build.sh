#!/usr/bin/env bash

##
## Copyright (C) 2020 Kyligence Inc. All rights reserved.
##
## http://kyligence.io
##
## This software is the confidential and proprietary information of
## Kyligence Inc. ("Confidential Information"). You shall not disclose
## such Confidential Information and shall use it only in accordance
## with the terms of the license agreement you entered into with
## Kyligence Inc.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
## "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
## LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
## A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
## OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
## SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
## LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
## DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
## THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
## (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
## OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
##

dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
if [ $# -ne 2 ] && [ $# -ne 3 ]; then
    echo "Usage: build.sh <tar package path> <base commit id> [<license_file>]"
    exit 1
fi

package_tar=$1
base_commit=$2
license_file=$3

working_dir=temp_hotfix
mkdir $working_dir
tar -zxvf ${package_tar} -C ${working_dir}  || exit 1

cp ${dir}/install.sh ${working_dir}     || exit 1

cd ${working_dir}
kap_dir=`ls|grep "kap-*"`
mv ${kap_dir} lib                       || exit 1
echo ${kap_dir} > name                  || exit 1
echo ${base_commit}@KAP > base_commit   || exit 1
cd lib

# copy license file
if [ ! -z "${license_file}" ]; then
    echo "delete existing license files:"
    if ls LICENSE 1> /dev/null 2>&1; then
        ls LICENSE
        rm LICENSE                        || exit 1
    fi
    cp $license_file .                  || exit 1
fi

# remove spark files
rm -rf spark
# remove tomcat files
cd tomcat
rm -rf bin lib temp work LICENSE RUNNING.txt RELEASE-NOTES NOTICE conf logs
cd ../../../

# add base conf and bin
mkdir ${working_dir}/base
current_branch=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p'| grep -v "detached")

echo "current_branch:$current_branch"
base_dir=$(cd ${dir}/../../;pwd)

git checkout $base_commit           || exit 1
git submodule update --init

if [ "$?" == "1" ]; then
    echo "Failed to checkout base commit. Please have a check."
    exit 1
fi

cp -rf ${base_dir}/kylin/build/bin ${working_dir}/base/                || exit 1
cp -rf ${base_dir}/kylin/build/conf ${working_dir}/base/               || exit 1
cp -rf ${base_dir}/build/bin/* ${working_dir}/base/bin/                         || exit 1
cp -rf ${base_dir}/build/conf/* ${working_dir}/base/conf/                       || exit 1
cp -rf ${base_dir}/kybot/build/kap/diag.sh ${working_dir}/base/bin/    || exit 1
if [[ ${kap_dir} =~ "plus" ]]; then
    cat ${working_dir}/base/conf/plus/kap-plus.min.properties >> ${working_dir}/base/conf/kap-plus.min.properties
    cat ${working_dir}/base/conf/plus/kap-plus.prod.properties >> ${working_dir}/base/conf/kap-plus.prod.properties
fi
rm -rf ${working_dir}/base/conf/plus

echo "git checkout ${current_branch}"
if [ ! -z "${current_branch}" ]; then
    git checkout $current_branch
    git submodule update --init
fi

mv ${working_dir} ${kap_dir}-hotfix                                 || exit 1
mkdir -p ${dir}/../../dist
tar -zcvf ${dir}/../../dist/${kap_dir}-hotfix.tar.gz ${kap_dir}-hotfix          || exit 1
rm -rf ${kap_dir}-hotfix

echo "Please find the hotfix package at: "
ls ${dir}/../../dist/${kap_dir}-hotfix.tar.gz