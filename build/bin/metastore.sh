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
    echo "       metastore.sh restore METADATA_RESTORE_PATH"
    echo "       metastore.sh backup-project PROJECT_NAME METADATA_BACKUP_PATH(the default path is KYLIN_HOME/meta_backups/)"
    echo "       metastore.sh restore-project PROJECT_NAME METADATA_RESTORE_PATH"
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


${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MaintainModeTool -on
if [ "$1" == "backup" ]
then
    BACKUP_OPTS="-backup"
    if [ $# -eq 2 ]; then
        path=`cd $2 && pwd -P`
        BACKUP_OPTS="${BACKUP_OPTS} -dir ${path}"
    elif [ $# -ne 1 ]; then
        help
    fi

    ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool ${BACKUP_OPTS}
    printBackupResult $?

elif [ "$1" == "restore" ]
then
    if [ $# -eq 2 ]; then
        path=`cd $2 && pwd -P`
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -restore -dir ${path}
        printRestoreResult $?
    else
       help
    fi

elif [ "$1" == "backup-project" ]
then
    BACKUP_OPTS="-backup"
    if [ $# -eq 3 ]; then
        path=`cd $3 && pwd -P`
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
        path=`cd $3 && pwd -P`
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -restore -dir ${path} -project $2
        printRestoreResult $?
    else
        help
    fi
else
    help
fi
${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MaintainModeTool -off


