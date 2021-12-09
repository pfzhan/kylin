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

function fetchFIZkInfo(){
    if [ -n "$FI_ENV_PLATFORM" ]
    then
        CLIENT_HIVE_URI=`env | grep "CLIENT_HIVE_URI"`
        array=(${CLIENT_HIVE_URI//// })
        verbose "FI_ZK_CONNECT_INFO is:${array[1]}"
        FI_ZK_CONNECT_INFO=${array[1]}

        zkConnectString=`$KYLIN_HOME/bin/get-properties.sh kylin.env.zookeeper-connect-string`
        if [ -z "$zkConnectString" ]
        then
            sed -i '$a\kylin.env.zookeeper-connect-string='$FI_ZK_CONNECT_INFO'' ${KYLIN_CONFIG_FILE}
        fi
    fi
}
fetchFIZkInfo