#!/bin/bash
# Kyligence Inc. License

# source me

# avoid re-entering
if [[ "$dir" == "" ]]
then
    dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
    # required because the entry shell can be $KYLIN_HOME/kybot/kybot.sh; must change /kybot to /bin
    dir=$(cd "$dir/../bin" && pwd -P)

    # misc functions
    function quit {
        echo "$@"
        if [[ -n "${QUIT_MESSAGE_LOG}" ]]; then
            echo `setColor 31 "$@"` >> ${QUIT_MESSAGE_LOG}
        fi
        exit 1
    }

    function verbose {
        if [[ -n "$verbose" ]]; then
            echo "$@"
        fi
    }

    function setColor() {
        echo -e "\033[$1m$2\033[0m"
    }

    function getValueByKey() {
        while read line
        do key=${line%=*} val=${line#*=}
        if [ "${key}" == "$1" ]; then
            echo $val
            break
        fi
        done<$2
    }

    # setup verbose
    verbose=${verbose:-""}
    while getopts ":v" opt; do
        case $opt in
            v)
                echo "Turn on verbose mode." >&2
                export verbose=true
                ;;
            \?)
                echo "Invalid option: -$OPTARG" >&2
                ;;
        esac
    done
    
    # set KYLIN_HOME with consideration for multiple instances that are on the same node
    CURRENT=`cd "${dir}/../"; pwd`
    if [[ "$CI_MODE" != "true" ]]; then
        [[ -z "$KYLIN_CONF" ]] || quit "KYLIN_CONF should not be set. Please leave it NULL, i.e. 'export KYLIN_CONF='"
        [[ -z "$KYLIN_HOME" ]] || [[ "${CURRENT}" == "${KYLIN_HOME}" ]] || quit "KYLIN_HOME=${KYLIN_HOME}, doesn't set correctly, please make sure it is set as current dir: ${CURRENT}, or leave it NULL, i.e. 'export KYLIN_HOME='"
    fi
    # have a check to avoid repeating verbose message
    if [[ "${KYLIN_HOME}" != "${CURRENT}" ]]; then
        export KYLIN_HOME=${CURRENT}
        verbose "KYLIN_HOME is ${KYLIN_HOME}"
    fi

    # set JAVA
    if [[ "${JAVA}" == "" ]]; then
        if [[ -z "$JAVA_HOME" ]]; then
            JAVA_VERSION=`java -version 2>&1 | awk -F\" '/version/ {print $2}'`
            if [[ $JAVA_VERSION ]] && [[ "$JAVA_VERSION" > "1.8" ]]; then
                JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
            else
                quit "Java 1.8 or above is required."
            fi
            [[ -z "$JAVA_HOME" ]] && quit "Please set JAVA_HOME"
            export JAVA_HOME
        fi
        export JAVA=$JAVA_HOME/bin/java
        [[ -e "${JAVA}" ]] || quit "${JAVA} does not exist. Please set JAVA_HOME correctly."
        verbose "java is ${JAVA}" 
    fi
fi
