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

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh $@

function checkFileOccupied() {
    target_file=$1
    pids="`fuser $target_file 2>&1`"
    if [[ "${pids}" == "" ]]; then
        echo false
    else
        echo true
    fi
}

function checkSizeExceedLimit() {
    target_file=$1
    file_threshold=`${KYLIN_HOME}/bin/get-properties.sh kylin.env.max-keep-log-file-threshold-mb`
    file_size=`du -b "$target_file" | cut -f 1`
    let file_threshold=file_threshold*1024*1024
    if [[ ${file_size} -gt ${file_threshold} ]]; then
        echo true
    else
        echo false
    fi
}

function logRotate() {
    target_file=$1
    # keep 10 history log files
    keep_limit=`${KYLIN_HOME}/bin/get-properties.sh kylin.env.max-keep-log-file-number`

    is_occupied=`checkFileOccupied ${target_file}`
    if [[ "${is_occupied}" == "true" ]]; then
        return
    fi

    is_too_large=`checkSizeExceedLimit ${target_file}`
    if [[ "${is_too_large}" == "false" ]]; then
        return
    fi

    if [[ -f $target_file ]]; then
        if [[ -f ${target_file}.${keep_limit} ]]; then
            # clean oldest log file first
            rm -f ${target_file}.${keep_limit}
        fi

        let p_cnt=keep_limit-1
        # renames logs .1 trough .${keep_limit}
        while [[ $keep_limit -gt 1 ]]; do
            if [ -f ${target_file}.${p_cnt} ] ; then
                mv -f ${target_file}.${p_cnt} ${target_file}.${keep_limit}
            fi
            let keep_limit=keep_limit-1
            let p_cnt=p_cnt-1
        done

        # rename current log to .1
        mv -f $target_file $target_file.1
    fi
}

ERR_LOG=${KYLIN_HOME}/logs/shell.stderr
OUT_LOG=${KYLIN_HOME}/logs/shell.stdout
KYLIN_OUT=${KYLIN_HOME}/logs/kylin.out

if [ "$1" == "start" ] || [ "$1" == "spawn" ]
then
    logRotate $ERR_LOG
    logRotate $OUT_LOG
    logRotate $KYLIN_OUT
fi