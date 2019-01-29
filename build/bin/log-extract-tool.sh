#!/bin/bash
# Kyligence Inc. License

function verbose() {
    (>&2 echo `date '+%F %H:%M:%S'` $@)
}

function help() {
    echo "Example usage:"
    echo "  log-extract-tool.sh -startTime <START_TIMESTAMP> -endTime <END_TIMESTAMP>"
    echo "  log-extract-tool.sh -query <QUERY_ID> [-c (optional, keep only clean query logs)]"
    echo "  log-extract-tool.sh -job <JOB_ID> [-c (optional, keep only clean job logs)]"
    exit 1
}

# accurate to minute
log_time_regex='^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2})'
function get_line_range() {
    local log_file=$1
    local category=$2
    local identifier=$3

    if [[ ${category} == '-job' ]]; then
        local pattern="${log_time_regex}(.*JobWorker.*jobidprefix:${identifier:0:8}.*)"

    elif [[ ${category} == '-query' ]]; then
        local pattern="${log_time_regex}(.*Query ${identifier}.*)"
    fi

    if [[ -n ${pattern} ]]; then
        local time_range=($(sed -nE "s/${pattern}/\1/p" ${log_file}))
        if [[ ${#time_range[@]} -le 0 ]]; then
            verbose "no found time range in $category $identifier"
            return 1
        fi
        start_time=${time_range[0]}
        end_time=${time_range[${#time_range[@]}-1]]}
    fi

    if [[ -z ${start_time} ]] || [[ -z ${end_time} ]]; then
        verbose "start_time or end_time illegal: [$start_time, $end_time]"
        return 1
    fi

    local line_nums=($(sed -nE -e "/^${start_time}/=" -e "/^${end_time}/=" ${log_file}))
    if [[ ${#line_nums[@]} -le 0 ]]; then
        verbose "no matched time range [$start_time, $end_time] in $log_file"
        return 1
    fi
    start_line_num=${line_nums[0]}
    end_line_num=${line_nums[${#line_nums[@]}-1]]}
#   the last line matched is the stack information
    end_line_num=$[${end_line_num}+100]
}

function filter_log_context() {
    local log_context=("$@")
    local category=${log_context[${#log_context[@]}-2]}
    local identifier=${log_context[${#log_context[@]}-1]}

    local thread_name=${log_context[3]}
    if [[ ${category} == '-job' ]]; then
        local pattern="JobWorker.*jobidprefix:${identifier:0:8}"

    elif [[ ${category} == '-query' ]]; then
        local pattern="Query ${identifier}"

    elif [[ ${category} == '-server' ]]; then
        return 0
    fi

    if [[ ${thread_name} =~ ${pattern} ]]; then
        return 0
    fi
    return 1
}

log_regex='([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}) ([^ ]*)[ ]+\[(.*)\] ([^: ]*) :([\n\r. ]*)'
function extract_local_log() {
    IFS=$'\n'
    local log_file=$1
    local category=$2
    local identifier=$3

    get_line_range ${log_file} ${category} ${identifier}
    if [[ $? -ne 0 ]]; then
        verbose "can not found line range in $log_file, exit"
        return 1
    fi

    verbose "start to extract [$log_file] category[$category] identifier[$identifier] [$start_time => $end_time]"

    if [[ -z $4 ]] && [[ $4 != "-c" ]]; then
        sed -n "${start_line_num},${end_line_num}p" ${log_file}
        return 0
    fi

    local log_context=()
    for line in $(sed -n "${start_line_num},${end_line_num}p" ${log_file}); do
        if [[ ${line} =~ ${log_regex} ]] && [[ ${#BASH_REMATCH[*]} == 6 ]]; then
            log_context=("${BASH_REMATCH[@]}")
            filter_log_context ${log_context[@]} ${category} ${identifier}
            if [[ $? != 0 ]]; then
                log_context=()
                continue
            fi
            echo ${line}
        else
            if [[ ${#log_context[*]} -gt 0 ]]; then
                echo ${line}
            fi
        fi
    done
}

function corrected_time_range() {
    IFS=$'\n'
    local log_file=$1
    for line in $(sed -n "1,1000p" ${log_file}); do
        if [[ ${line} =~ ${log_regex} ]] && [[ ${#BASH_REMATCH[*]} == 6 ]]; then
            first_log_time=${BASH_REMATCH[1]}
            first_log_time=${first_log_time:0:16}
            break
        fi
    done

    if [[ -n $first_log_time ]] && [[ $start_time < $first_log_time ]]; then
        verbose "corrected start time from [$start_time] to [$first_log_time]"
        start_time=$first_log_time
    fi

    last_modified=$(date -r $log_file "+%F %H:%M")
    if [[ $end_time > $last_modified ]]; then
        verbose "corrected end time from [$end_time] to [$last_modified]"
        end_time=$last_modified
    fi

}

function main() {
    if [[ $# -lt 3 ]]; then
        help
    fi

    if [[ $2 == "-query" ]] || [[ $2 == "-job" ]]; then
        extract_local_log $@
        exit $?

    elif [[ $# -eq 5 ]] && [[ $2 == "-startTime" ]] && [[ $4 == "-endTime" ]]; then
        if [[ "$(uname)" == "Darwin" ]]; then
            start_time=$(date -r $[$3 / 1000] "+%F %H:%M")
            end_time=$(date -r $[$5 / 1000] "+%F %H:%M")
        else
            start_time=$(date -d "@$[$3 / 1000]" "+%F %H:%M")
            end_time=$(date -d "@$[$5 / 1000]" "+%F %H:%M")
        fi

        corrected_time_range $1
        extract_local_log $1 "-server" "_"
        exit $?
    else
        help
    fi
}

main $@