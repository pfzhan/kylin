#!/bin/bash
# Kyligence Inc. License

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
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -backup -dir ${KYLIN_HOME}/meta_backups
    elif [ $# -eq 2 ]; then
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -backup -dir $2
    else
        help
    fi

elif [ "$1" == "restore" ]
then
    if [ $# -eq 2 ]; then
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -restore -dir $2
    else
       help
    fi

elif [ "$1" == "backup-project" ]
then
    if [ $# -eq 2 ]; then
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -backup -project $2 -dir ${KYLIN_HOME}/meta_backups
    elif [ $# -eq 3 ]; then
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -backup -project $2 -dir $3
    else
        help
    fi

elif [ "$1" == "restore-project" ]
then
    if [ $# -eq 3 ]; then
        ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.MetadataTool -restore -project $2 -dir $3
    else
        help
    fi
else
    help
fi

