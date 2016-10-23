#!/bin/bash
pkg_name=`ls dist/*.tar.gz|grep -v orig.tar.gz$`
echo $pkg_name
if [ -f test.license ]; then
    tar -zxvf $pkg_name
    cp -f test.license kap-*/
    rm -f $pkg_name
    tar -zcvf $pkg_name kap-*/
    rm -rf kap-*/
fi

orig_pkg_name=`ls dist/*.tar.gz|grep orig.tar.gz$`
if [ -f test.license ]; then
    tar -zxvf $orig_pkg_name
    cp -f test.license kap-*/
    rm -f $orig_pkg_name
    tar -zcvf $orig_pkg_name kap-*/
    rm -rf kap-*/
fi

test_home=smoke-test
mkdir ${test_home}

# copy test scripts to test home
cp -rf build/smoke-test/* ${test_home}/

# run test scripts
bash ${test_home}/smoke-test.sh ${pkg_name} ${orig_pkg_name} ${test_home}    || { exit 1; }

# clear test home
rm -rf ${test_home}