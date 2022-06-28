#!/bin/bash

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

RESTORE='\033[0m'
YELLOW='\033[00;33m'
if [ -z $KYLIN_HOME ]; then
  export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

function printComputeMetricsInfoResult() {
  error=$1
  if [[ $error == 0 ]]; then
    echo -e "Measure finished. Detailed Message is at \"logs/shell.stderr\""
  else
    echo -e "Measure failure. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
  fi
}

metricDate=${1}
if [ -z ${metricDate} ]; then
  metricDate=$(date -d "yesterday" +%Y%m%d)
fi

echo "Specifies the date(${metricDate}) on which metrics are calculated"

function computeMetricsInfo() {
  ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetricsInfoTool -date ${metricDate}
  printComputeMetricsInfoResult $?
}

computeMetricsInfo
