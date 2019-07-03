#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh

echo "Packing for Newten..."

export PACKAGE_SPARK=1
export SKIP_FRONT=0
export SKIP_OBF=0
for PARAM in $@; do
    if [ "$PARAM" == "-noSpark" ]; then
        echo "Skip packaging Spark..."
        export PACKAGE_SPARK=0
        shift
    fi

    if [[ "$PARAM" == "-skipObf" ]]; then
        echo "Skip Obfuscation..."
        export SKIP_OBF=1
        shift
    fi

    if [[ "$PARAM" == "-skipFront" ]]; then
        echo 'Skip install front-end dependencies...'
        export SKIP_FRONT=1
        shift
    fi
done



# Make share commands exist in environment
echo "BUILD STAGE 1 - Checking environment..."
checkCommandExists mvn
checkCommandExists git
checkCommandExists npm

exportProjectVersions

kap_commit_sha1=`git rev-parse HEAD`
echo "${kap_commit_sha1}@KAP" > build/commit_SHA1
if [ -z "$BUILD_SYSTEM" ]; then
    BUILD_SYSTEM="MANUAL"
fi
echo "Build with ${BUILD_SYSTEM} at" `date "+%Y-%m-%d %H:%M:%S"` >> build/commit_SHA1


cat > build/CHANGELOG.md <<EOL
### Release History

#### KE 4.0.0 release note

**Feature & Enhancement**

- Optimize source table schema changes
- Add record and management for index usage
- Support rapid sampling of wide tables for multiple columns
- Automatically detect queue memory and set the number of concurrent tasks
- Adjust the built-in metabase to PostgreSQL
- Expand the range of queries that can be accelerated

EOL


KAP_VERSION_NAME="Kyligence Enterprise ${release_version}"

echo "${KAP_VERSION_NAME}" > build/VERSION
echo "VERSION file content:" ${KAP_VERSION_NAME}

echo "BUILD STAGE 2 - Build binaries..."
sh build/script_newten/build.sh $@             || { exit 1; }

echo "BUILD STAGE 3 - Prepare spark..."
sh build/script_newten/download-spark.sh      || { exit 1; }

echo "BUILD STAGE 4 - Prepare influxdb..."
sh build/script_newten/download-influxdb.sh      || { exit 1; }

echo "BUILD STAGE 5 - Prepare grafana..."
sh build/script_newten/download-grafana.sh      || { exit 1; }

echo "BUILD STAGE 6 - Prepare postgresql..."
sh build/script_newten/download-postgresql.sh      || { exit 1; }

echo "BUILD STAGE 7 - Generate license..."
sh build/script_newten/generate-license.sh || { exit 1; }

echo "BUILD STAGE 8 - Prepare and compress package..."
sh build/script_newten/prepare.sh ${MVN_PROFILE} || { exit 1; }
sh build/script_newten/compress.sh               || { exit 1; }

echo "BUILD STAGE 9 - Clean up..."
    
echo "BUILD FINISHED!"
