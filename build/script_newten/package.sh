#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh


# restore the removals if noPlus mode ran before
for file in extensions/core-common/src/main/resources/kylin-defaults0.properties build/conf/profile_min/kylin.properties
do
    restoreKAPPlusConfigs $file
done

# restore raw table to sample cube template
mv -f ./sample_raw_table_bak/* extensions/examples/sample_cube/template/
    
echo "Packing for Newten..."

export PACKAGE_SPARK=1
export SKIP_FRONT=0
for PARAM in $@; do
    if [ "$PARAM" == "-noSpark" ]; then
        echo "Skip packaging Spark..."
        export PACKAGE_SPARK=0
        shift
    fi

    if [[ "$PARAM" == "-skipFront" ]]; then
        echo 'Skip install front-end dependencies...'
        export SKIP_FRONT=1
        shift
    fi
done


for PARAM in $@; do
    if [ "$PARAM" == "cdh5.7" ]; then
        export MVN_PROFILE="cdh5.7"
        break
    fi
done

# Make share commands exist in environment
echo "BUILD STAGE 1 - Checking environment..."
checkCommandExits mvn
checkCommandExits git
checkCommandExits npm

exportProjectVersions

kap_commit_sha1=`git rev-parse HEAD`
echo "${kap_commit_sha1}@KAP" > build/commit_SHA1
if [ -z "$BUILD_SYSTEM" ]; then
    BUILD_SYSTEM="MANUAL"
fi
echo "Build with ${BUILD_SYSTEM} at" `date "+%Y-%m-%d %H:%M:%S"` >> build/commit_SHA1


cat > build/CHANGELOG.md <<EOL
### Release History

#### Newten 0.0.1 release note

**Feature & Enhancement**

- Newten version 0.0.1


EOL


KAP_VERSION_NAME="Newten ${release_version}"

echo "${KAP_VERSION_NAME}" > build/VERSION
echo "VERSION file content:" ${KAP_VERSION_NAME}

echo "BUILD STAGE 2 - Build binaries..."
sh build/script_newten/build.sh $@             || { exit 1; }

echo "BUILD STAGE 3 - Prepare spark..."
sh build/script_newten/download-spark.sh      || { exit 1; }

echo "BUILD STAGE 4 - Prepare and compress package..."
sh build/script_newten/prepare.sh ${MVN_PROFILE} || { exit 1; }
sh build/script_newten/compress.sh               || { exit 1; }

echo "BUILD STAGE 5 - Clean up..."

# restore the removals if noPlus mode ran before
for file in src/core-common/src/main/resources/kylin-defaults0.properties build/conf/profile_min/kylin.properties
do 
    restoreKAPPlusConfigs $file
done
    
# restore raw table to sample cube template
BAK=`ls | grep raw_table`
if [ ! -z "$BAK" ]; then
	mv -f raw_table* src/examples/sample_cube/template/
fi	

echo "BUILD FINISHED!"
