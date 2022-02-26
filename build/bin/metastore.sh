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

RESTORE='\033[0m'
YELLOW='\033[00;33m'

if [ -z $KYLIN_HOME ];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

function help {
    echo "usage: metastore.sh backup METADATA_BACKUP_PATH(the default path is KYLIN_HOME/meta_backups/)"
    echo "       metastore.sh restore METADATA_RESTORE_PATH [--after-truncate]"
    echo "       metastore.sh backup-project PROJECT_NAME METADATA_BACKUP_PATH(the default path is KYLIN_HOME/meta_backups/)"
    echo "       metastore.sh restore-project PROJECT_NAME METADATA_RESTORE_PATH [--after-truncate]"
    exit 1
}

function printBackupResult() {
    error=$1
    if [[ $error == 0 ]]; then
        if [[ -z "${path}" ]]; then
            path="\${KYLIN_HOME}/meta_backups"
        fi
        echo -e "${YELLOW}Backup at local disk succeed.${RESTORE}"
    else
        echo -e "${YELLOW}Backup failed. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
    fi
}

function printRestoreResult() {
    error=$1

    if [[ $error == 0 ]]; then
        echo -e "${YELLOW}Restore succeed. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
    else
        echo -e "${YELLOW}Restore failed. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
    fi
}

function printEnterMaintainModeResult() {
    echo -e "${YELLOW}Enter Maintain Mode succeed. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
}

function printExitMaintainModeResult() {
    echo -e "${YELLOW}Exit Maintain Mode succeed. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
}

function check_path_empty() {
  # this function is to check whether the path is an empty str.
    if [ -z "$1" ]; then
          exit 1
    fi
}

function check_path_empty_dir() {
  # this function is to check whether the path is an empty directory.
    if [ -z "$(ls -A "$1")" ]; then
        echo -e "${YELLOW}The directory \"$1\" is an empty directory, please check.${RESTORE}"
        exit 1
    fi
}

function turn_on_maintain_mode() {
  ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MaintainModeTool -on -reason 'metastore tool' -hidden-output true
  local ret=$?
  if [[ $ret != 0 ]]; then
      echo -e "${YELLOW}Enter Maintain Mode failed. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
      exit $ret
  fi
}

function turn_off_maintain_mode() {
    ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MaintainModeTool -off -hidden-output true
    local ret=$?
    if [[ $ret != 0 ]]; then
        echo -e "${YELLOW}Exit Maintain Mode failed. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
        exit $ret
    fi
}

function restore_all() {
        local path=`cd $1 && pwd -P`
        check_path_empty ${path}
        check_path_empty_dir ${path}
        turn_on_maintain_mode
        printEnterMaintainModeResult
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -restore -dir ${path} ${2}
        printRestoreResult $?
        turn_off_maintain_mode
        printExitMaintainModeResult
}

function restore_project() {
        local path=`cd $1 && pwd -P`
        check_path_empty ${path}
        check_path_empty_dir ${path}
        turn_on_maintain_mode
        printEnterMaintainModeResult
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -restore -dir ${path} -project ${2} ${3}
        printRestoreResult $?
        turn_off_maintain_mode
        printExitMaintainModeResult
}


if [ "$1" == "backup" ]
then
    BACKUP_OPTS="-backup"
    if [ $# -eq 2 ]; then
        path=`cd $2 && pwd -P`
        check_path_empty ${path}
        BACKUP_OPTS="${BACKUP_OPTS} -dir ${path}"
    elif [ $# -ne 1 ]; then
        help
    fi

    ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool ${BACKUP_OPTS}
    printBackupResult $?

elif [ "$1" == "restore" ]
then
    if [ $# -eq 2 ]; then
        restore_all ${2}
    elif [ $# -eq 3 -a "$3" == "--after-truncate" ]; then
        restore_all ${2} "--after-truncate"
    else
       help
    fi

elif [ "$1" == "backup-project" ]
then
    BACKUP_OPTS="-backup"
    if [ $# -eq 3 ]; then
        path=`cd $3 && pwd -P`
        check_path_empty ${path}
        BACKUP_OPTS="${BACKUP_OPTS} -dir ${path}"
    elif [ $# -ne 2 ]; then
        help
    fi
    BACKUP_OPTS="${BACKUP_OPTS} -project $2"
    ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool ${BACKUP_OPTS}
    printBackupResult $?

elif [ "$1" == "restore-project" ]
then
    if [ $# -eq 3 ]; then
        restore_project ${3} ${2}
    elif [ $# -eq 4 -a "$4" == "--after-truncate" ]; then
        restore_project ${3} ${2} "--after-truncate"
    else
        help
    fi
else
    help
fi


