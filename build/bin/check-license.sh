#!/bin/bash
# Kyligence Inc. License
#title=Checking License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

# step1. check exist
LICENSE_FILE=${KYLIN_HOME}/LICENSE
[[ -f $LICENSE_FILE ]] || quit "Can't detect LICENSE file, please contact Kyligence Inc."

# step2. check format
EXPECT_FLAG_1=`grep -rw '====' ${LICENSE_FILE}`
EXPECT_FLAG_2=`grep -rw 'Kyligence Analytics' ${LICENSE_FILE}`

[[ -n "${EXPECT_FLAG_1}" ]] && [[ -n "${EXPECT_FLAG_2}" ]] || quit "LICENSE file is incorrect, please contact Kyligence Inc."