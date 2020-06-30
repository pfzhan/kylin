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

function verbose() {
    (>&2 echo `date '+%F %H:%M:%S'` $@)
}

function help() {
    echo "Example usage:"
    echo "  sql-extract-tool.sh -file <LOG_FILE_PATH> "
    exit 1
}

function extract_sql() {
    log_file=$2
    log_file_dir=`dirname ${log_file}`
    sql_file=${log_file_dir}/kylin_sql_`date '+%F_%H:%M:%S'`.txt
    sql_file_tmp=${sql_file}.tmp
    if [ -f ${sql_file} ];then
        rm -rf ${sql_file}
    fi

    if [ -f ${sql_file_tmp} ];then
        rm -rf ${sql_file_tmp}
    fi

    verbose "start to extract sql from [${log_file}] to [${sql_file}]"

    sql=""
    sql_start=0
    sql_end=0
    while read line
    do

    if [[ $line == SQL:* ]];then
        sql_start=1
        sql=${line:4}
    elif [[ $line == User:* ]];then
        sql_end=1
    elif [ $sql_start == 1 ] && [ $sql_end == 0 ];then
        sql="$sql $line"
    elif [[ $line == "Success: true" ]];then
        echo "$sql;" >> ${sql_file_tmp}
        sql_start=0
        sql_end=0
        sql=""
    else
        sql_start=0
        sql_end=0
        sql=""
    fi
    done < ${log_file}

    row_count=$(cat ${log_file} | wc -l)
    verbose "file [${log_file}] total scan row count : ${row_count}"

    sort -u ${sql_file_tmp} >> ${sql_file}

    sql_count=$(cat ${sql_file} | wc -l)
    verbose "file [${log_file}] total extract sql num : ${sql_count}"

    rm -rf ${sql_file_tmp}
    sql_file_size=$(ls -lah ${sql_file} | awk '{ print $5}')
    verbose "extract sql succeed, the sql file size is ${sql_file_size}"
    split -d -C 4M ${sql_file} "${sql_file}-"
}

function main() {
    if [[ $# -lt 2 ]]; then
        help
    fi

    if [[ $1 == "-file" ]]; then
        extract_sql $@
        exit $?
    else
        help
    fi
}

main $@
