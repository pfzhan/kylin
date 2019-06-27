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

metadata_tool="-cp ${KYLIN_HOME}/tool/kap-tool-*.jar -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties io.kyligence.kap.tool.MetadataTool"

if [ "$1" == "backup" ]
then
    if [ $# -eq 1 ]; then
        path="${KYLIN_HOME}/meta_backups"
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -backup -dir ${path}
        ret=$?
    elif [ $# -eq 2 ]; then
        path=`cd $2 && pwd -P`
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -backup -dir ${path}
        ret=$?
    else
        help
    fi

elif [ "$1" == "restore" ]
then
    if [ $# -eq 2 ]; then
        path=`cd $2 && pwd -P`
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -restore -dir ${path}
        ret=$?
    else
       help
    fi

elif [ "$1" == "backup-project" ]
then
    if [ $# -eq 2 ]; then
        path="${KYLIN_HOME}/meta_backups"
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -backup -dir ${path} -project $2
        ret=$?
    elif [ $# -eq 3 ]; then
        path=`cd $3 && pwd -P`
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -backup -dir ${path} -project $2
        ret=$?
    else
        help
    fi


elif [ "$1" == "restore-project" ]
then
    if [ $# -eq 3 ]; then
        path=`cd $3 && pwd -P`
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -restore -dir ${path} -project $2
        ret=$?
    else
        help
    fi
else
    help
fi


if [[ ${ret} -eq "11" ]]
then
    echo -e "${YELLOW}Restore failed. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
elif [[ ${ret} -eq "12" ]]
then
    echo -e "${YELLOW}Backup failed. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
elif [[ ${ret} -eq "1" ]]
then
    echo -e "${YELLOW}Backup at local disk succeed. The backup path is ${path}.${RESTORE}"
elif [[ ${ret} -eq "2" ]]
then
    echo -e "${YELLOW}Backup succeed. The backup path is ${path}.${RESTORE}"
    echo -e "${YELLOW}The backup process is delegated to a running job server. If it is a remote server, the backup will not be on local disk.${RESTORE}"
elif [[ ${ret} -eq "3" ]]
then
    echo -e "${YELLOW}Restore succeed. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
fi

