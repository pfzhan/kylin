#!/bin/bash
# Kyligence Inc. License

# This script is for production metadata store manipulation
# It is designed to run in hadoop CLI, both in sandbox or in real hadoop environment
#
# If you're a developer of Kylin and want to download sandbox's metadata into your dev machine,
# take a look at SandboxMetastoreCLI


source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

function help {
    echo "usage: metastore.sh backup"
    echo "       metastore.sh fetch DATA"
    echo "       metastore.sh reset"
    echo "       metastore.sh refresh-cube-signature"
    echo "       metastore.sh restore PATH_TO_LOCAL_META"
    echo "       metastore.sh list RESOURCE_PATH"
    echo "       metastore.sh cat RESOURCE_PATH"
    echo "       metastore.sh remove RESOURCE_PATH"
    echo "       metastore.sh clean [--delete true]"
    echo "       metastore.sh backup-project PROJECT_NAME PATH_TO_LOCAL_META_DIR"
    echo "       metastore.sh backup-cube CUBE_NAME PATH_TO_LOCAL_META_DIR"
    echo "       metastore.sh restore-project PROJECT_NAME PATH_TO_LOCAL_META_DIR"
    echo "       metastore.sh restore-cube PROJECT_NAME PATH_TO_LOCAL_META_DIR"
    echo "       metastore.sh promote PATH_TO_LOCAL_META_DIR"
    exit 1
}

if [ "$1" == "backup" ]
then

    if [ "$#" == "2"  ] && [ "$2" == "--noSeg" ]
    then
        ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.CubeMetaExtractor -compress false -allProjects -destDir ${KYLIN_HOME}/meta_backups -includeSegments false
    else
        mkdir -p ${KYLIN_HOME}/meta_backups

        _now=$(date +"%Y_%m_%d_%H_%M_%S")
        _file="${KYLIN_HOME}/meta_backups/meta_${_now}"
        echo "Starting backup to ${_file}"
        mkdir -p ${_file}

        ${KYLIN_HOME}/bin/kylin.sh ResourceTool download ${_file}
        echo "metadata store backed up to ${_file}"
    fi


elif [ "$1" == "fetch" ]
then

    _file=$2

    _now=$(date +"%Y_%m_%d_%H_%M_%S")
    _fileDst="${KYLIN_HOME}/meta_backups/meta_${_now}"
    echo "Starting restoring $_fileDst"
    mkdir -p $_fileDst

    ${KYLIN_HOME}/bin/kylin.sh ResourceTool fetch $_fileDst $_file
    echo "metadata store backed up to $_fileDst"

elif [ "$1" == "restore" ]
then

    _file=$2
    echo "Starting restoring $_file"
    ${KYLIN_HOME}/bin/kylin.sh ResourceTool upload $_file

elif [ "$1" == "list" ]
then

    _file=$2
    echo "Starting list $_file"
    ${KYLIN_HOME}/bin/kylin.sh ResourceTool list $_file

elif [ "$1" == "remove" ]
then

    _file=$2
    echo "Starting remove $_file"
    ${KYLIN_HOME}/bin/kylin.sh ResourceTool remove $_file

elif [ "$1" == "cat" ]
then

    _file=$2
    echo "Starting cat $_file"
    ${KYLIN_HOME}/bin/kylin.sh ResourceTool cat $_file

elif [ "$1" == "reset" ]
then

    ${KYLIN_HOME}/bin/kylin.sh ResourceTool  reset
    
elif [ "$1" == "clean" ]
then

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.MetadataCleanupJob  "${@:2}"
    
elif [ "$1" == "refresh-cube-signature" ]
then

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.cube.cli.CubeSignatureRefresher

elif [ "$1" == "backup-project" ]
then

    if [ "$#" -ne 3 ]; then
        help
    fi

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.CubeMetaExtractor -compress false -project $2 -destDir $3 -includeSegments false

elif [ "$1" == "backup-cube" ]
then
    if [ "$#" -ne 3 ]; then
        help
    fi

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.CubeMetaExtractor -compress false -cube $2 -destDir $3 -includeSegments false

elif [ "$1" == "restore-project" ]
then
    if [ "$#" -ne 3 ]; then
        help
    fi

    TMP_NAME=${RANDOM}"-"${RANDOM}"-"${RANDOM}"-"${RANDOM}"-"${RANDOM}
    cp -rf $3 $TMP_NAME
    zip -r $TMP_NAME.zip $TMP_NAME
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.CubeMetaIngester -srcPath $TMP_NAME.zip -project $2 -overwriteTables true -forceIngest true
    rm -rf $TMP_NAME
    rm -f $TMP_NAME.zip

elif [ "$1" == "restore-cube" ]
then
    if [ "$#" -ne 3 ]; then
        help
    fi

    TMP_NAME=${RANDOM}"-"${RANDOM}"-"${RANDOM}"-"${RANDOM}"-"${RANDOM}
    cp -rf $3 $TMP_NAME
    zip -r $TMP_NAME.zip $TMP_NAME
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.CubeMetaIngester -srcPath $TMP_NAME.zip -project $2 -overwriteTables true -forceIngest true
    rm -rf $TMP_NAME
    rm -f $TMP_NAME.zip
	
elif [ "$1" == "promote" ]
then
    if [ "$#" -ne 2 ]; then
        help
    fi

    ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.metadata.MetadataUpgrader $2

else
    help
fi

