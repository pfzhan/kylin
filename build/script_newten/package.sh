#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh

echo "Packing for Newten..."

export PACKAGE_TIMESTAMP=1
export PACKAGE_SPARK=1
export SKIP_FRONT=0
export SKIP_OBF=0
for PARAM in $@; do
    if [ "$PARAM" == "-noTimestamp" ]; then
        echo "Package without timestamp..."
        export PACKAGE_TIMESTAMP=0
        shift
    fi

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

#### Kyligence Enterprise 4.0.6 release note

**Feature**

- Read/Write Separation Deployment
- Update license in Web UI
- Apply for license by self-service in Marketplace

**Enhancement**

- Support to view Spark execution details after executing SQL query
- Support intersect count function in expert mode
- Optimize the building performance if the fact table is a view and contains the count distinct measure
- Support using table index to answers some aggregate queries
- The select * query can return computed columns when there are computed columns saved in the model

**Bugfix**

- After restarting the job, the job log output is not updated in time in the job monitor page. It is still displayed as the log of the previous execution

#### Kyligence Enterprise 4.0.5 release note

**Feature**

- Expert mode supports recommendation mode, which provides recommendations for existing models by analyzing the query history and model usage.
- Provide different user roles to meet different levels of operation permissions
- Provide health check API to check node status in cluster deployment

**Enhancement**

- Support to accelerate queries with count(constant) or count(distinct constant)
- Support to input 'database.table' when loading source tables
- Support to set the default database at project level
- Enable Tomcat compression configuration by default to reduce the query results and network transmission time when integrating with a BI tool

**Bugfix**

- The index building job fails if it contains a count distinct measure and the source table is empty.
- Pushdown query fails if there are same expressions in select clause.
- Incremental loading job fails, when the partition column contains empty value.
- Merging job may fail in resource detection steps if there are too many segments

#### Kyligence Enterprise 4.0.4 release note

**Feature**

- Support cell level data access control

**Enhancement**

- Support exporting query results to a CSV file
- Support system admin user to view all job information across all projects in the job monitor page
- Support some new query functions
- Optimize the recognition and conversion of computed columns in smart mode project
- Support searching database.table in data source loading page
- Provide model/index group expansion rate

**Bugfix**

- Project information has not been updated after switching to other projects in the model editing page.
- In expert mode, computed columns are unavailable to use when defining table index

#### Kyligence Enterprise 4.0.3 release note

**Enhancement**

- Support automatic upgrade via script
- Enhance diagnostic package
- Support some new query functions
- Optimize the recognition and conversion of computed columns in smart mode project
- Simplify integration with Kerberos
- Optimize the build performance of Count Distinct measure

**Bugfix**

- Queries still hit the cache when the model goes offline
- In smart mode, when the type of measure is int or boolean, query pushdown in Power BI will return error
- In smart mode, when querying the recommended computed column through Power BI, query pushdown will return error
- Timestamp() function cannot support millisecond precision

#### Kyligence Enterprise 4.0.2 release note

**Feature**

- Support integration with LDAP
- Support integration with 3rd-party User Authentication System
- Support HTTPS connection

**Enhancement**

- Support to view partial job execution log and download the full logs on monitor page
- Support some new query functions
- Improve the query performance when queries contain the join condition with a high cardinality column
- Support dynamic resource allocation in table sampling job
- Optimize the recognition and conversion of computed columns in smart mode project

**Bugfix**

- The password reset script for ADMIN user doesn’t take effect

#### Kyligence Enterprise 4.0.1 release note

**Feature & Enhancement**

- Optimize table encoding
- Optimize building
- Optimize TPCH query
- Improve speed of table sampling
- Improve step information of job monitor page

**Bugfix**

- Cannot stop instance using command kylin.sh stop
- Count distinct cannot serialization
- The task of deleting the model cannot be restarted and cannot be deleted
- Wrong data range can be saved
- Configuring incorrect metadata, the process is still running after the startup fails

#### Kyligence Enterprise 4.0.0 release note

**Feature & Enhancement**

- Metastore
  - Built-in PostgreSQL as metastore
- Data Source
  - Integrate Hive as default data source
  - Quick table sampling in minutes level
  - If source schema changes, the system can detect missing column and impacted model
- Model
  - Intuitive model design canvas to define table relations, dimensions, measures
  - Intelligent modeling based on user's query pattern
  - Quick search for model semantic layer as dimensions, measures, tables, joins, etc. in the model editing page
  - Aggregate index for OLAP queries
  - Table index to query on raw data
- Load Data
  - Full load data
  - Incremental load data by date / time
  - Segment management after loading data
- Query Pushdown
  - Enable smart query pushdown by default to explore massive data in minutes
- Query Acceleration
  - One-click to pre-calculate target SQL patterns
  - Import query history logs or detect user query behavior in the system to summarize frequently used SQL patterns
  - Custom acceleration rules to focus on different SQL patterns
- High Availability Deployment
- Compatible Hadoop Distributions
  - Cloudera CDH 5.8 / 5.12
  - Hortonworks HDP 2.4
  - Huawei FusionInsight C70 / C80
- Standard JDBC driver and standard ODBC driver
- Integrate with multiple BI tools
  - Tableau Desktop 8.2.2 / 9.0
  - Tableau Server 10.3
  - Power BI Desktop 2.70.5494.761
  - Power BI Service
  - IBM Cognos Framework 10
  - IBM Cognos Server 10.2 for Linux
  - QlikView 11.20.13405.0
  - OBIEE 11g / 12c
  - SAP BO Web Intelligence 4.1
  - SmartBI Insight
  - FineBI and FineReport
- Integrate with Excel and Python
- Operation And Diagnosis
  - Environment check tool to ensure installation dependency and autority are pre-settled properly
  - Storage management at the project level and one-click to clean up low usage storage
  - System and job diagnoses tool to help trouble shooting
  - Operation preference at the project level to automate daily tasks
  - Dynamic resources allocation and workload balance on job execution
- Security
  - User and group management
  - Integrate with Kerberos
  - Audit log for metadata changes

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

echo "BUILD STAGE 7 - Prepare and compress package..."
sh build/script_newten/prepare.sh ${MVN_PROFILE} || { exit 1; }
sh build/script_newten/compress.sh               || { exit 1; }

echo "BUILD STAGE 8 - Clean up..."
    
echo "BUILD FINISHED!"
