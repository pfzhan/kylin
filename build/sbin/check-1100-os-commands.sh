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

#title=Checking OS Commands

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking OS commands..."

command -v lsb_release                  || echo "${CHECKENV_REPORT_PFX}WARN: Command lsb_release is not accessible. Please run on Linux OS."
a=`lsb_release -a`                      || echo "${CHECKENV_REPORT_PFX}WARN: Command 'lsb_release -a' does not work. Please run on Linux OS."
[[ $a == *Mac* ]]                       && echo "${CHECKENV_REPORT_PFX}WARN: Mac is not officially supported. Use at your own risk."
[[ $a == *Ubuntu* ]]                    && echo "${CHECKENV_REPORT_PFX}WARN: Ubuntu is not officially supported. Use at your own risk."

command -v hadoop                       || quit "ERROR: Command 'hadoop' is not accessible. Please check Hadoop client setup."
if [[ $(hadoop version) != *"mapr"* ]]
then
    command -v hdfs                         || quit "ERROR: Command 'hdfs' is not accessible. Please check Hadoop client setup."
fi
command -v yarn                         || quit "ERROR: Command 'yarn' is not accessible. Please check Hadoop client setup."