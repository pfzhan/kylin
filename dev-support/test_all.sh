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

dir=$(dirname ${0})
cd ${dir}/..

set -o pipefail
rm testall*.log

export SPARK_HOME=${SPARK_HOME:-"/root/spark-2.1.1-bin-hadoop2.6"}
#export ZIPKIN_HOSTNAME=${ZIPKIN_HOSTNAME:-"localhost"}
#export ZIPKIN_SCRIBE_PORT=${ZIPKIN_SCRIBE_PORT:-"9410"}
#export KAP_HDFS_WORKING_DIR=${KAP_HDFS_WORKING_DIR:-"/kylin"}
#export hdpv=${HDP_VERSION:-"2.4.0.0-169"}

mvn clean install -DskipTests                                       2>&1 | tee testall-1.log  || exit 1
mvn test -fae                                                       2>&1 | tee testall-2.log  || exit 1
