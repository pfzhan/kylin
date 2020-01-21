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

#### Kyligence Enterprise 4.0.11 release note

**Feature**

- Optimize Hadoop platform compatibility: Support Huawei FusionInsight 6.5.1

**Enhancement**

- Optimize the query functions:

- - Support a new measure collect_set(column)
  - Support unix_timestamp, concat_ws function

- Improve user experience

  - Optimize the interactions of filtering in the list

  - Add a GIF in model editing page to guide the modeling

- Safety and stability

  - Provide a CLI tool to clean up invalid files on HDFS

  - Support for metadata URL in load balancer configuration

- Optimize API function list

  - Support to define partition column

  - Support to get job list

**Bugfix**

- In cluster deployment mode, the query node can't work properly as well if the All node is unavailable.
- In FI 6.5.1, there are no spark_logs in the full diagnostic package
- In FI 6.5.1, check-1600-spark-dir.sh fails to modify hive-scratch permissions
- Connect Kyligence MDX failed if the model contains computed columns
- Building job fails, when computed column expression contains double quotes
- When the model contains a TopN measure, the build job may fail with ERROR: Comparison method violates its general contract!
- When using the SQL file to create models, the model advisor page appears blank if the SQL can be answered by a snapshot of other models.
- When the "TableName_ColumnName" of different tables can be spliced as the same string, it will return an error "Duplicate column names" when accelerating the query
- Some queries may fail to accelerate when the partial match of the model is enabled and multiple queries can be answered by the same model
- The sampling data result is incorrect when the source table contains null values
- The number of use should not be null if the index has never been hit
- The index ID in the recommendation list may have duplicated values
- The recommendation list is not up-to-date after manually deleting the index which is suggested as being deleted
- The Admin page may occur an exception when there is no project exists
- When the user name is long, the GUI may display an exception
- The User Guide cannot be executed when the user name contains spaces
- Cannot distinguish multiple spaces in the user/user group name
- The related table index is not deleted after deleting the table in the model
- Reload table fails when the partition column of the model is deleted from the source table
- The content of the "Answered By" column may not show completely in the query history page
- Cannot execute the SQL again after copying it from the query history page
- After cloning a model created by other users, the owner of the new cloned model is incorrect

#### Kyligence Enterprise 4.0.10 release note

**Feature**

- Optimize Hadoop platform compatibility: Support Hortonworks Data Platform 3.1 and Cloudera CDH 6.1

**Enhancement**

- Support to use offset clause after limit clause in queries
  - Note: Pushdown is not available for such queries in current version
- Support to set system-level parameters to specify the default precision of data type decimal
- Optimize query performance when the query contains a large number of union clauses
- Improve user experience
  - When adding dimensions in batch add, if the dimension list is collapsed, it can prompt duplicate names in the dimension list when submitting
  - Fix the table header when batch adding measures
  - Optimize the drop-down box display when selecting a column, and display the full column name when hovering
- Optimize and supplement query details information display
  - Add the index ID of the query hit into the query details
  - Add query ID into the query history page
- Optimize API function list
  - Support to delete a specified segment
  - Add the Job ID in the return information of the build job API
  - Provide model description API

**Bugfix**

- After the version upgrade, if editing the model and saving, the indexes are all rebuilt
  - Scope of influence: upgrade from historical version to 4.0.9

- Kyligence Enterprise fails to execute backup script
- When the source table information does not change, reloading the source table will clear the snapshot
- It fails to create new models when joining multiple identical dimension tables in the query
- When using SQLs to create models, it fails when the same table is defined as a fact table and a dimension table at the same time in different queries
- When the computed column refers to a column in the dimension table, and the computed column has the same name as the referenced column, it fails to build index
- Query pushdown fails when the query contains placeholders
- The query fails to hit the model when using strings in window functions *LEAD* and *LAG*
- Query fails with null parameter in *IF* and *CONCAT* functions
- The *CONCAT* function cannot concatenate string and numeric parameters
- When querying a column of time type, the result of querying through JDBC is not consistent with the result in Kyligence Enterprise
- A nested query fails when the result of the subquery window function is used in the window function after the select statement
- The model cannot be set to no partition if deleting partition columns in the source table and reloading it
- Dimension table cannot modify alias after using columns from dimension table in index
- When the query result contains multiple columns and exceeds the browser width, all query results cannot be viewed on the web UI

#### Kyligence Enterprise 4.0.9 release note

**Enhancement**

- Support more flexible index definitions. Measures can be added into a specified aggregate group
- Optimize the recommendation strategies of AI Augmented Mode, and adopt different garbage clean rules for customized and recommended indices
- Optimize the index treemap, and use different block areas and colors to represent the data size and usage
- Support data filter conditions when saving models
- Providing index protection when the index refreshing job is running
- Support project administrator to manage project and data access control

**Bugfix**

- After manually creating tables in metastore, it still failed to start
- When the query contains the POSITION function, the query results are inaccurate
- When the query contains the window function LAG (), the query fails when no sort is added
- When the query contains a window function, all columns in the table will be selected during parsing so the existing model cannot be matched
- Query may fail if the join condition is in a subquery and used in an expression simultaneously
- When the same column is used repeatedly in the select clause and the subquery, the query may report an error with Reference 'xxx' is ambiguous
- When the query contains sum (case when in ()), the acceleration fails
- When granting row ACL to different user groups, the permission relationship should be OR
- Username cannot contain spaces in LDAP
- Status filtering in the job page may not take effect
- Non-root users may fail to get table metadata when starting Kyligence Enterprise and using JDBC driver

#### Kyligence Enterprise 4.0.8 release note

- **Enhancement**
- In AI Augmented Mode, support custom SQL to generate new models, indices and optimize existing models
- In AI Augmented Mode, remove the upper limit of dimensions in the aggregation group
- Support deleting indices manually
- Add source and reason information in the recommendation page
- Provide basic Rest API
- Support filtering query status in query history page

**Bugfix**

- The raw query result may inaccurate if a table is defined as the fact table and the lookup table in different models and also used in table index
- The table index editing page cannot be shown properly in low resolution
- Queries cannot be accelerated when comparing time type columns with string type columns
- Diagnosis package cannot be generated if the log file is too large
- Precise Count Distinct measure (bitmap) cannot be changed to Approximate Count Distinct (hllc) after saving the model
- Query may fail if the window is used in filter condition and the subquery contains order by clause

#### Kyligence Enterprise 4.0.7 release note

**Enhancement**

- Support ShardBy column in aggregate index, which could distribute data into multiple shards to improve query performance
- Improve the information structure of aggregate index page and provide more details such as index source
- Support to choose the time format of partition column
- Improve the stability of editing aggregate index page when there are too many aggregate groups or combination of dimensions
- Reduce some unnecessary outputs in console to avoid confusion while generating the diagnostic package.
    > Note:  The diag.log will still contain the complete records.

**Bugfix**

- After deleting a project, metadata may not be cleaned up totally

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
