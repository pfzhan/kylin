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

if [ -z "$KYLIN_HOME" ]; then
    quit 'Please make sure KYLIN_HOME has been set'
else
    echo "KYLIN_HOME is set to ${KYLIN_HOME}"
fi

if [[ $# -gt 1 ]]; then
    echo "Usage: install.sh -notConf"
    exit 1
fi

copy_conf_flag=1
if [ "$-notConf" == "$1" ]; then
    copy_conf_flag=0
fi

cd ${dir}

# validate commit
cat $KYLIN_HOME/commit_SHA1 | grep $(cat base_commit)
if [ "$?" -ne "0" ]; then
    echo "Your KAP version is not compatible with this hotfix."
    exit 1
fi

hotfix_name=`cat name`
backup_dir=backup/${hotfix_name}
patch_dir=patch
copy_dir=copy

if [ "$copy_conf_flag" == "1" ]; then
    # detect merge conflict in conf and bin
    rm -rf $patch_dir $copy_dir
    mkdir -p $patch_dir/conf $patch_dir/bin
    mkdir $copy_dir
    cp -r lib/conf copy
    cp -r lib/bin copy

    for f in `cd ${dir}/lib/conf;ls`; do
        if [ -f "$KYLIN_HOME/conf/$f" ]; then
            diff -Na ${dir}/base/conf/$f $KYLIN_HOME/conf/$f > $patch_dir/conf/$f.patch
            if [ -s "$patch_dir/conf/$f.patch" ]; then
                patch -sfl $copy_dir/conf/$f $patch_dir/conf/$f.patch
                if [ "$?" -ne "0" ]; then
                    echo "Cannot auto-merge conf and bin files. Please run following command to install without copying conf and bin files, and then manually merge them: install.sh -noConf "
                    exit 1
                fi
            else
                rm $patch_dir/conf/$f.patch;
            fi
        fi
    done
    for f in `cd ${dir}/lib/bin;ls`; do
        if [ -f "$KYLIN_HOME/bin/$f" ]; then
            diff -Na ${dir}/base/bin/$f $KYLIN_HOME/bin/$f > $patch_dir/bin/$f.patch
            if [ -s "$patch_dir/bin/$f.patch" ]; then
                patch -sfl $copy_dir/bin/$f $patch_dir/bin/$f.patch
                if [ "$?" -ne "0" ]; then
                    echo "Cannot auto-merge conf and bin files. Please run following command to install without copying conf and bin files, and then manually merge them: install.sh -noConf "
                    exit 1
                fi
            else
                rm $patch_dir/bin/$f.patch;
            fi
        fi
    done
fi

# backup
if [ -d ${backup_dir} ]; then
    rm -rf ${backup_dir}
fi
mkdir -p ${backup_dir}

if [ "$copy_conf_flag" == "1" ]; then
    mv $KYLIN_HOME/bin ${backup_dir}/
    mv $KYLIN_HOME/conf ${backup_dir}/
fi

mv $KYLIN_HOME/lib ${backup_dir}/
mv $KYLIN_HOME/commit_SHA1 ${backup_dir}/
mv $KYLIN_HOME/sample_cube ${backup_dir}/
mv $KYLIN_HOME/LICENSE ${backup_dir}/
mkdir -p ${backup_dir}/tomcat
mv $KYLIN_HOME/tomcat/webapps ${backup_dir}/tomcat/

# copy
cp -r lib/lib $KYLIN_HOME/
cp -r lib/commit_SHA1 $KYLIN_HOME/
cp -r lib/sample_cube $KYLIN_HOME/
cp -r lib/LICENSE $KYLIN_HOME/
cp -r lib/tomcat/webapps $KYLIN_HOME/tomcat
if [ "$copy_conf_flag" == "1" ]; then
    cp -r $copy_dir/conf $KYLIN_HOME/
    cp -r $copy_dir/bin $KYLIN_HOME/
fi

echo "Hotfix installed successfully!"