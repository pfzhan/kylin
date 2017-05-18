#!/bin/bash
# Kyligence Inc. License

# source me

verbose=${verbose:-""}

while getopts ":v" opt; do
    case $opt in
        v)
            echo "Turn on verbose mode." >&2
            verbose=true
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            ;;
    esac
done

if [[ "$dir" == "" ]]
then
    dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

    function quit {
        echo "$@"
        if [[ -n "${QUIT_MESSASGE_LOG}" ]]; then
            echo `setColor 31 "$@"` >> ${QUIT_MESSASGE_LOG}
        fi
        exit 1
    }

    [[ -z $KYLIN_CONF ]] || quit "KYLIN_CONF should not be set. Please leave it NULL, i.e. 'export KYLIN_CONF='"
    # set KYLIN_HOME with consideration for multiple instances that are on the same node
    CURRENT=${CURRENT:-"${dir}/../"}
    CURRENT=`cd "$CURRENT"; pwd`

    [[ -z ${KYLIN_HOME} ]] || [[ ${CURRENT} == ${KYLIN_HOME} ]] || quit "KYLIN_HOME=${KYLIN_HOME}, doesn't set correctly, please make sure it is set as current dir: ${CURRENT}, or leave it NULL, i.e. 'export KYLIN_HOME='"

    export KYLIN_HOME=${CURRENT}
    dir="$KYLIN_HOME/bin"

    function verbose {
        if [[ -n "$verbose" ]]; then
            echo "$@"
        fi
    }

    function setColor()
    {
        echo -e "\033[$1m$2\033[0m"
    }

    function getValueByKey()
    {
        while read line
        do key=${line%=*} val=${line#*=}
        if [ "${key}" == "$1" ]; then
            echo $val
            break
        fi
        done<$2
    }

fi
