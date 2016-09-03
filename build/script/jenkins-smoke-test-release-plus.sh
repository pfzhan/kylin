#!/bin/bash
pkg_name=`ls dist/*-obf.tar.gz`
echo $pkg_name
if [ -f test.license ]; then
    tar -zxvf $pkg_name
    mv -f test.license kylin-kap-*-bin/
    rm -f $pkg_name
    tar -zcvf $pkg_name kylin-kap-*-bin/
    rm -rf kylin-kap-*-bin/
fi

test_home=smoke-test
mkdir ${test_home}

# copy test scripts to test home
cp -rf kylin/build/smoke-test/* ${test_home}/
cp -rf build/smoke-test/* ${test_home}/

# run test scripts
bash ${test_home}/smoke-test-plus.sh ${pkg_name} ${test_home}    || { exit 1; }

# clear test home
rm -rf ${test_home}