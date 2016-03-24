#!/bin/bash
pkg_name=`ls dist/*.tar.gz`

test_home=smoke-test-${BUILD_TAG}
mkdir smoke-test-${BUILD_TAG}

# copy test scripts to test home
cp -rf kylin/smoke-test/* smoke-test-${BUILD_TAG}/

# run test scripts
bash ${test_home}/smoke-test.sh ${pkg_name} ${test_home}

# clear test home
rm -rf smoke-test-${BUILD_TAG}