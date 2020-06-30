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

python -V 2>&1 || quit "ERROR: Detect python version failed. Please have python 2.7.7 or above installed."

version=`python -V 2>&1`
version_num="$(echo ${version} | cut -d ' ' -f2)"
version_first_num="$(echo ${version_num} | cut -d '.' -f1)"
version_second_num="$(echo ${version_num} | cut -d '.' -f2)"
version_third_num="$(echo ${version_num} | cut -d '.' -f3)"

if [[ "$version_first_num" -lt "2" ]] || [[ "$version_second_num" -lt "7" ]] || [[ "$version_third_num" -lt "7" ]]; then
    echo "Error: python 2.7.7 or above is required for query history migration "
    exit 0
fi

# install required packages
python -m pip install influxdb
python -m pip install mysql-connector-python
python -m pip install psycopg2-binary
python -m pip install tzlocal

python query_history_upgrade.py $@