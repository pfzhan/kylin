#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

if [ -z $KYLIN_SRC_HOME ];then
    KYLIN_SRC_HOME=kylin/
fi

echo "Apache Kylin source repository: ${KYLIN_SRC_HOME}"
