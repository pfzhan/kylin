#!/usr/bin/env bash
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
                patch -sfl $copy_dir/conf/$f $patch_dir/conf/$f.patch || echo "Cannot auto-merge conf and bin files. Please run following command to install without copying conf and bin files, and then manually merge them: install.sh -noConf "
            else
                rm $patch_dir/conf/$f.patch;
            fi
        fi
    done
    for f in `cd ${dir}/lib/bin;ls`; do
        if [ -f "$KYLIN_HOME/bin/$f" ]; then
            diff -Na ${dir}/base/bin/$f $KYLIN_HOME/bin/$f > $patch_dir/bin/$f.patch
            if [ -s "$patch_dir/bin/$f.patch" ]; then
                patch -sfl $copy_dir/bin/$f $patch_dir/bin/$f.patch  || echo "Cannot auto-merge conf and bin files. Please run following command to install without copying conf and bin files, and then manually merge them: install.sh -noConf "
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
mkdir ${backup_dir}

if [ "$copy_conf_flag" == "1" ]; then
    mv $KYLIN_HOME/bin ${backup_dir}/
    mv $KYLIN_HOME/conf ${backup_dir}/
fi

mv $KYLIN_HOME/lib ${backup_dir}/
mv $KYLIN_HOME/commit_SHA1 ${backup_dir}/
mv $KYLIN_HOME/sample_cube ${backup_dir}/
mv $KYLIN_HOME/*.license ${backup_dir}/
mkdir -p ${backup_dir}/tomcat
mv $KYLIN_HOME/tomcat/webapps ${backup_dir}/tomcat/

# copy
cp -r lib/lib $KYLIN_HOME/
cp -r lib/commit_sha1 $KYLIN_HOME/
cp -r lib/sample_cube $KYLIN_HOME/
cp -r lib/*.license $KYLIN_HOME/
cp -r lib/tomcat/webapps $KYLIN_HOME/tomcat
if [ "$copy_conf_flag" == "1" ]; then
    cp -r $copy_dir/conf $KYLIN_HOME/
    cp -r $copy_dir/bin $KYLIN_HOME/
fi