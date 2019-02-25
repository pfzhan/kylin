#!/bin/bash
# Kyligence Inc. License

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

    for line in $(sed -n '/^SQL:/=' ${log_file}); do
        check_line=$[$line + 2]
        query_success=$(sed -n "${check_line}p" ${log_file} | awk '{print $2}')
        #echo $query_success
        if [ "${query_success}" == "true" ]
        then
           sql=$(sed -n "${line}p" ${log_file} | awk -F ':' '{print $2}')
           #echo ${sql}
           echo ${sql}";" >> ${sql_file_tmp}
        fi
    done
    sort -u ${sql_file_tmp} >> ${sql_file}
    rm -rf ${sql_file_tmp}
    sql_file_size=$(ls -lah ${sql_file} | awk '{ print $5}')
    verbose "extract sql succeed, the sql file size is ${sql_file_size}"
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