#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh



if [ "$1" == "-skipObf" ] || [ "$2" == "-skipObf" ]; then
    export SKIP_OBF=1
    shift
    echo "Skip Obfuscation..."
fi

if [ "$1" == "-noPlus" ] || [ "$2" == "-noPlus" ]; then
    export PACKAGE_PLUS=0
    
    # remove all the "KAP plus only stuff"
    for file in extensions/core-common/src/main/resources/kylin-defaults0.properties build/conf/profile_min/kylin.properties
    do 
        removeKAPPlusConfigs $file
    done
    
    # remove raw table from sample cube template
    mkdir -p ./sample_raw_table_bak/
    mv -f extensions/examples/sample_cube/template/raw_table* ./sample_raw_table_bak/
    
    shift
    
    echo "Packing for KAP Normal..."
else
	export PACKAGE_PLUS=1
	
	# restore the removals if noPlus mode ran before
    for file in extensions/core-common/src/main/resources/kylin-defaults0.properties build/conf/profile_min/kylin.properties
    do 
        restoreKAPPlusConfigs $file
    done

    # restore raw table to sample cube template
    mv -f ./sample_raw_table_bak/* extensions/examples/sample_cube/template/
    
	echo "Packing for KAP Plus..."
fi

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

if [ ! -d "kylin" ]; then
    echo "Apache Kylin source not found."
    exit 1
fi
exportProjectVersions

kap_commit_sha1=`git rev-parse HEAD`
kylin_commit_sha1=`git submodule status kylin`
kylin_commit_sha1=${kylin_commit_sha1:1:40}
echo "${kap_commit_sha1}@KAP" > build/commit_SHA1
echo "${kylin_commit_sha1}@ApacheKylin" >> build/commit_SHA1
if [ -z "$BUILD_SYSTEM" ]; then
    BUILD_SYSTEM="MANUAL"
fi
echo "Build with ${BUILD_SYSTEM} at" `date "+%Y-%m-%d %H:%M:%S"` >> build/commit_SHA1


cat > build/CHANGELOG.md <<EOL
### Release History

#### KAP 2.5.3 release note

**Bugfix**

- Fix the issue cannot save streaming model

#### KAP 2.5.2 release note

**Feature & Enhancement**

- Support multi-partition modeling
- Enable fine grained segment management
- Support export pushdown query
- Enhanced migrating tool-migrate cube metadata and cube segment
- Metadata check tool, help find and clean isolated metadata
- Upgrade KyBot Client to 1.1.24: support latest KAP
- Support login kyligence account
- Support integrating with Active directory

 **Bugfix**

- Fix union query error
- Fix filtering non-ascii query error
- Fix segment auto merge error (count distinct measure including high carnality column)
- Fix distcp command failed frequently issue on read/write separation deploy
- Fix HDFS check failed issue on read/write separation deploy
- JDBC connecting lost issue when using MySQL to store metadata
- Sampling can tolerant the value exceeding precision

#### KAP 2.5.1 release note

**Bugfix**

- Fix the storage path issue on read/write separation deploy
- Fix model check failed on no-partition model
- Fix return invalid column issue on prepare statements of ODBC driver

#### KAP 2.5.0 release note

**Feature & Enhancement**

- ACL on project/ table/ cell
- Model adviser to enable users generate new models via SQL
- Support users verify SQL on model/cube before building
- Enhanced cube optimizer with multi-preferences suggestions
- Enrich sample data with streaming model and cube
- Enable users to turn on/off the raw query capability by cube
- Enhanced query error message, clarifying confliction among SQLs and model/cube

**Bugfix**

- Prepare statement returns null through JDBC connecting
- Metadata broken caused job list returns null
-  Loading unstable when sync hive tables
- Fix the issues about configuring read/write separated settings on Azure
-  Checking duplication and null value on primary key of lookup tables
- Convention issues on computed column
- Fix issues when query pushdown working on some rare sub-queries
- Support "explain plan for" to illustrate query execution plan

**KyAnalyzer**

**Feature & Enhancement**

- KyAnalyzer will read license directly from KAP

**Bugfix**

- Cube with Hierarchy in aggregation group can be synced correctly
- Non-admin user can now sync Cube correctly
- Non-admin user query Cube with their own access permission applied

#### KAP 2.4.7 release note

**Bugfix**

- Fix the trim bug on SQL query preparing

#### KAP 2.4.6 release note

**Feature & Enhancement**

- Trim TupleFilter after dictionary-based filter optimization([KYLIN-2823](https://issues.apache.org/jira/browse/KYLIN-2823))
- Separate querying resource between cube and table index

**Bugfix**

- Fix BufferOverflowException when sampling table
- Fix the storage presenting error after cube build([KYLIN-2834](https://issues.apache.org/jira/browse/KYLIN-2834))
- Fix return null values when union two subquery under join([KYLIN-2830](https://issues.apache.org/jira/browse/KYLIN-2830))
- Fix join condition push down issue caused by type casting([KYLIN-2773](https://issues.apache.org/jira/browse/KYLIN-2773))
- Fix FileSystem chaos when setup Kylin on HDInsight([KYLIN-2766](https://issues.apache.org/jira/browse/KYLIN-2766))

#### KAP 2.4.5 release note

**Feature & Enhancement**

- Allow editing the model if it is not conflict with cube definition
- Query pushdown supports queries as "create table" and "drop table"

**Bugfix**

- Fix the issue table not found when sampling on view based table

#### KAP 2.4.4 release note

**Feature & Enhancement**

- Enhanced computed column supports defining expression cross tables

**Bugfix**

- Fix the clean up tool's error on CDH5.2.x
- Fix the issue when query result contains Chinese characters



#### KAP 2.4.3 release note

**Feature & Enhancement**

- Speed up and optimize AGG editing interaction
- Refine Max dimension combination(Joint group) calculation



#### KAP 2.4.2 release note

**Bugfix**

- Ignore the job that can't be parsed in KapStorageCleanupCLI

- Fix the issue when one computed column is named after an existing column


#### KAP 2.4.1 release note

**Feature & Enhancement**

- Cross Hadoop cluster migration tool
- Upgrade attached Spark1.6.3 to Spark 2.1.1
- Support license update on KAP web page
- Support default password reset
- Refine Cube design process
  - Distinct dimensions in the AGG groups
  - Support edit return type of measure
- Support query skip database prefix from query pushdown([KYLIN-2758](https://issues.apache.org/jira/browse/KYLIN-2758))



**Bugfix**

- Sync issue when reload existing hive table([KYLIN-2754](https://issues.apache.org/jira/browse/KYLIN-2754))
- Get "Owner required" error on saving data model([KYLIN-2762](https://issues.apache.org/jira/browse/KYLIN-2762))
- Fix exception of NoSuchFieldError occurs when do storage clean up
- Add SQL injection check on filter condition of models
- Fix the broken cube cannot be deleted issue([KYLIN-2691](https://issues.apache.org/jira/browse/KYLIN-2691))
- Fix cube suggested measure return type mismatches its original return type
- Fix the issue query contains Chinese characters, error while exporting result as csv
- Fix the issue ArrayIndexOutOfBoundsException: -1 raised when set Filter with primary key



EOL


if [ "${PACKAGE_PLUS}" = "1" ]; then
    KAP_VERSION_NAME="KAP Enterprise Plus ${release_version}"
else
    KAP_VERSION_NAME="KAP Enterprise ${release_version}"
fi
echo "${KAP_VERSION_NAME}" > build/VERSION
echo "VERSION file content:" ${KAP_VERSION_NAME}

echo "BUILD STAGE 2 - Build binaries..."
sh build/script/build.sh $@             || { exit 1; }

echo "BUILD STAGE 3 - Prepare tomcat..."
sh build/script/download-tomcat.sh      || { exit 1; }

echo "BUILD STAGE 4 - Prepare spark..."
sh build/script/download-spark.sh      || { exit 1; }


echo "BUILD STAGE 5 - Prepare and compress package..."
sh build/script/prepare.sh ${MVN_PROFILE} || { exit 1; }
sh build/script/compress.sh               || { exit 1; }

echo "BUILD STAGE 6 - Clean up..."

# restore the removals if noPlus mode ran before
for file in extensions/core-common/src/main/resources/kylin-defaults0.properties build/conf/profile_min/kylin.properties
do 
    restoreKAPPlusConfigs $file
done
    
# restore raw table to sample cube template
BAK=`ls | grep raw_table`
if [ ! -z "$BAK" ]; then
	mv -f raw_table* extensions/examples/sample_cube/template/
fi	

echo "BUILD FINISHED!"
