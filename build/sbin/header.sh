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

# source me

function isValidJavaVersion() {
    version=`java -version 2>&1 | awk -F\" '/version/ {print $2}'`
    version_first_part="$(echo ${version} | cut -d '.' -f1)"
    version_second_part="$(echo ${version} | cut -d '.' -f2)"

    if [[ "$version_first_part" -eq "1" ]] && [[ "$version_second_part" -lt "8" ]]; then
        echo "false"
        exit 0
    fi

    echo "true"
}

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
            if [[ `isValidJavaVersion` == "true" ]]; then
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

    if [[ -z ${MAPR_HOME} ]];then
        export MAPR_HOME="/opt/mapr"
    fi
fi
