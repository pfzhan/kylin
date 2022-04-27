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

if [ -n "$BIGDATA_HOME" ]
then
    export FI_ENV_PLATFORM=$BIGDATA_HOME
fi

if [ -n "$BIGDATA_CLIENT_HOME" ]
then
    export FI_ENV_PLATFORM=$BIGDATA_CLIENT_HOME
fi

if [ -n "$FI_ENV_PLATFORM" ]
then
   zookeeper_jars=$(find $FI_ENV_PLATFORM/HDFS/hadoop/share/hadoop/common -maxdepth 2 \
   -name "zookeeper-3.5.6-hw-ei-302*.jar" -not -name "*test*" \
   -o -name "zookeeper-jute-3.5.6-hw-ei-302*.jar" -not -name "*test*")

   if [ -n "$zookeeper_jars" ]
   then
     export IS_MRS_PLATFORM='true'
   fi
fi

