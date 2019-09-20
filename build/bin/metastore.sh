#!/bin/bash
# Kyligence Inc. License

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
    local=`isBackupInLocal`

    if [[ $error == 0 ]]; then
        if [[ $local == 0 ]]; then
            echo -e "${YELLOW}Backup at local disk succeed. The backup path is ${path}.${RESTORE}"
        else
            echo -e "${YELLOW}Notice: If Query node and All node are deploied in differents servers, the backup process will be delegated to the All node and the backup files will be saved under All node directory.${RESTORE}"
            echo -e "${YELLOW}Backup succeed. The backup path is ${path}.${RESTORE}"
        fi
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

function isBackupInLocal() {
    ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.CuratorOperator $1 2>/dev/null
    echo $?
}

if [ "$1" == "backup" ]
then
    if [ $# -eq 1 ]; then
        path="${KYLIN_HOME}/meta_backups"
    elif [ $# -eq 2 ]; then
        path=`cd $2 && pwd -P`
    else
        help
    fi

    ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -backup -dir ${path}
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
    if [ $# -eq 2 ]; then
        path="${KYLIN_HOME}/meta_backups"
    elif [ $# -eq 3 ]; then
        path=`cd $3 && pwd -P`
    else
        help
    fi

     ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -backup -dir ${path} -project $2
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

