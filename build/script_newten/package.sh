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

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh

echo "Packing for KE..."

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

#### Kyligence Enterprise 4.1.4 release note

**Enhancement**

- Improve product usability
  - When major editors of models that need to rebuild indexes are submitted, optimize the prompt information, and add a second confirmation operation and guidance
  - Improve index optimization prompt information
  - When saving model, optimize whether to add index button information
  - Optimize startup script log information
  - Improve the color contrast between the disabled state and the normal state of the component to facilitate the user to distinguish
  - Optimized the display structure of the project page
- Rest API
  - Provide a public API for project-level authentication management
  - Provide public API for data authentication management
  - Provide public API for user and user group management
- Others
  - When there is only one ALL node, provide a switch to support selective shutdown of the multi-active feature of the construction node
  - Decouple the checksum of the partition format and the pushdown query

**Bugfix**

- When logging in with integrated third-party authentication, the error message is incorrect
- Jump from the task interface to the model page, the reset button is invalid
- When the number of projects exceeds 100, the error message contains the "Unknown Error Code" field
- View table sampling causes memory overflow
- The ER diagram of the model occasionally appears after switching tabs and cannot be displayed
- The system enters maintenance mode, modify the advanced settings of the model interface, no pop-up reminder
- In cancel the read-only mode API,  when the user name and password are incorrect, it can be cancelled too
- In project-level authentication managemen API, when there is no project, the error message is mixed in Chinese and English
- In Return to user list API, the is_case_sensitive and page_offset parameter cannot take effect
- Change the fact table, choose to save and build the index, no new build task is generated

#### Kyligence Enterprise 4.1.3 release note

**Enhancement**

- Add support for yyyy-MM, yyyyMM  time partition format in incremental build
- When executing the check_env.sh script, before creating the metadata table, check whether the metadata table length meets the standard
- Improve product stability
  - When the build task includes high base column topN measure, there is a high probability of memory overflow
- Improve product usability
  - In the model selection filter, the last month is changed to the last 30 days to eliminate ambiguity
  - Before importing SQL modeling files, clearly indicate the format requirements
  - When editing the aggregation group, confirm the popup before cancelling or page jump

**Bugfix**

- Computed columns in the model are used as a detailed index, query including computed columns cannot hit the model
- In SQL modeling model recommendation, the display of recommended indexesnumber does not correct
- The incremental build selection time range is one day, and the end time includes hours. After the build is completed, the model is broken
- There are problems with the TopN construction data, so some queries with Limit cannot display some columns
- When the check quota is stuck in the cloud environment, the build task cannot be scheduled
- For query result line maximum configuration, boundary value processing error
- For project item maximum value configuration, boundary value check error
- Improve product stability
  - On searching the dimension page, when you enter a space, the page keeps flashing
  - On model optimization suggestion interface, when there are too many suggestions, the browser is stuck or even crashes
  - When generating the diagnostic package on the interface, if diagnostic package is too large and the memory is insufficient, an error of Out of Memory is reported
  - Zookeeper service is unstable when the job node is multi-active, resulting in inaccessible front-end pages
  - When the build task failed, continuous retry resulted in a large number of temporary files
  - When SQL modeling, too much SQL statements are imported at one time, the process crashes
- Rest API
  - In return to the model list API, the create_time in the returned information is not accurate
  - In return to the user list, the project parameter does not take effect
  - In return model list  and return model description information API,  last_modified time is inconsistent with metabase
- When there are no project items, there will be two pop-ups indicating that there are no items
- On task interface, unknown error is reported in select all operation
- Error occurs when directly reloading the table without sampling
- The loading status of  incremental load button is displayed incorrectly
- The action of modifying the default database cannot be saved

#### Kyligence Enterprise 4.1.2 release note

**Feature**

- Support to answer some SUM (Expression) queries by models

**Enhancement**

- Improve product security
  - Standardize the generation of session ID, and improve uniqueness, randomness and security
  - Fix some potential security problems, and add interface parameter verification
    - Provide configuration to control whether to enable cross-origin
  - Filing the Linux operation permissions required for each subdirectory under the installation directory
  - Support to customize the network address bound to the Kyligence Enterprise service to prevent possible monitoring vulnerabilities
  - Support encrypted storage of session ID in the database
  - After the cumulative number of incorrect password entries exceeds a certain value, login will be locked
  - Improve user login password encryption algorithm
- Improve Rest API
  - Add parameter to query API, and support to force pushdown when pushdown switch is turned on
- Improve log and diagnostic package
  - Provide log which records user login information, system start and stop information and upgrade information, which will enhance problem traceability
- Improve product usability
  - Add explanations to queries can be accelerated in acceleration page
  - When there is no active All node, before editing a page or form, checking will be provided to prevent the loss of edited content
- Others
  - Provide a migration script tool to support the migration of query history from Influxdb to RDBMS
    - This tool is an auxiliary tool for upgrading and will not be made public. For use, please contact Kyligence Technical Support
  - Optimize metadata migration tool to support metadata migration from Kyligence Enterprise 3.x to Kyligence Enterprise 4.1

**Bugfix**

- When the memory is relatively large and the core is relatively small, there may be inaccurate resource detection, resulting in the table sampling task can not be carried out normally
- When the cluster is unstable, the same task may be scheduled by multiple schedulers, resulting in failure
- When the column name contains '.', the regular matching error during the build process causes the column conversion to fail, resulting in the failure of resource detection in the building
- Table loading fails when the source table contains columns of data type *Map*
  > - Known limitation: Columns with data types *Map*, *Array*, *Struct*, and *Binary* will be skipped when loading the table. At the same time, these types of column can still get results when query pushdown, but the results may not be correct
- Fix the problem of inconsistent front-end and back-end verification in forms
- Fix the problem that the timeout exit mechanism does not fully take effect
- Error occurs when adding the computed column which is shaped like a column multiplied by a decimal.
- When the partition column type is *string* and the selected time format is *yyyy-MM-dd HH: mm: ss: SSS*, automatic acquisition of the time range or manual triggering of incremental building may fail
- Synchronization result of the model owner by the mirror tool may be wrong
- Configuration with no default value in the configuration file are read by the upgrade script, resulting in an error
- The task of regularly accelerating the query history takes up much memory, which may cause the regularly triggered building jobs to fail to execute normally
- When query contains ceil(floor(column_name to HOUR) to HOUR)，the result between query pushdown and query hitting model is inconsistent
- Some of the mechanisms in the breakpoint resume of the building job may cause high latency of the building job on the cloud
- When accelerating more than 10,000 SQLs at a time, the instance service may be unstable
- When the metadata table name is too long, the query history may fail to refresh
- When using the view table as a fact table for incremental building, there is no data filtering based on the selected time range
- By using the aggregation index API to pass in the columns not included in the model, inconsistencies will happen between the columns between model and aggregate index
- After the random password configuration is closed, the password still needs to be changed when logging in to the web UI

#### Kyligence Enterprise 4.1.1 release note

**Product behavior changes**

- Accelerating notification will be offline for the time being

  > When there are too many queries to be accelerated, the interface of the acceleration notification is prone to time out, resulting in process buildup and affecting system stability.

- The project-level metadata backup function on the web UI will be offline for the time being

  > When the project-level metadata is backed up through the web UI, the backup path is the same as the system-leve automatic backup metadata path, which may cause the system metadata to be overwritten, making the query service unavailable

**Enhancement**

- Support to load Hive tables created by skipping the first row, which will support Kyligence Cloud to realize the function of reading the first row as column name
- Support to start Spark query service asynchronously when starting Kyligence
- Provide segment verification API (v4 version) to help users check whether the corresponding interval already exists in the current model before building
- Optimize index recommendation mechanism to reduce some possible over-fitting problems
- Support to modify storage quota through API and web UI
- Optimize error codes and replace manual configuration with system enumeration to ensure uniqueness
- Optimize the delay time of metadata synchronization between Query node and All node to reduce the inconsistency of product views between nodes
- Provide the management user with the authority of data source management, which will support Kyligence Cloud to realize self-service analysis scenarios
- Improve product security
  - Support to control whether details will show in pop-up windows when error occurs through configuration items in `kylin.properties`, to prevent possible security vulnerabilities
  - Increase the complexity of ADMIN user initialization random password
- Improve log and diagnostic package
  - When generating a diagnostic package through the command line, support for removing metadata to control the size of the diagnostic package
  - Add Spark event log which records Sparder building events to the diagnostic package, guaranteeing sufficient log information for problem diagnosis
- Improve product usability
  - Provide ER diagram, dimension information and measurement information in the model overview, so that users can view the model definition in the non-edited state
  - Support to modify the owner of the project and model on the web UI
  - Support to delete index in batches on the web UI
- Improve user experience
  - Optimize the pop-up window height of project permissions setting and table row and column level permissions setting
  - Optimize the loading style of the content under the model, and use the loading state instead of the empty state

**Bugfix**

- In a single All node deployment mode, a large number of build failures may occur
- When the data source table is a view, it takes a long time to detect the partition column format, which may cause out of memory
- Fix user enumeration security holes
  - Unify error text when the password is incorrect and the user name does not exist, preventing enumerating accounts that already exist in the system
- Fix some v2 API interface compatibility issues, including inconsistent time zones, inconsistent return results, etc.
- When the source table is a view and the data size is quite large, the resource detection step may not work properly and the sampling fails
- When the computed column contains double quotes, reloading the related fact table may fail
- An unknown error text will show when the wrong password is entered on the login page
- *Degree* function and *Sign* function are available in query pushdown, but the corresponding index cannot be recommended when accelerating
- Fix configuration error in Grafana page
- In AWS environment, when the build task fails, the restart task may report an error FileNotFoundException
- Index building may be slow when it contains *TopN* measure
- In the cluster deployment mode, after creating an administrator user on the Query node, delete the user and create a common user with the same name. When using this user to log in, it still shows as administrator authority
- Fix problems of failed queries
  - When the query contains an *offset* clause, the query may fail
  - After JDBC uses *setTimestamp* to pass parameters, when the query precision is milliseconds, no data is returned
  - When performing complex filtering on derived dimensions, the query may fail
  - Queries with *similar to* clauses may fail
  - Queries containing conditions like *where xx is false* will fail
- Fix problems of inaccurate query results
  - The result precision of the *Floor* function and *Ceil* function when they hit the model is inconsistent with the results when the query is push down
  - In some models synchronized by the metadata mirror tool, the build range of the segment in metadata and that on HDFS may be inconsistent, resulting in incorrect query results
  - When double quotes are used for the alias of a column in a subquery, the return result of the corresponding column is not case sensitive
- Fix English error text in Chinese interface
  - When the format of the selected time partition column is temporarily not supported, an English error text appears in the Chinese interface
  - When merging discontinuous segments, an English error text appears in the Chinese interface
  - After failing to obtain the time partition column format automatically or failing to build incrementally, an English error text appears in the Chinese interface
  - After the number of models, projects, etc. reaches the upper limit, an English error text appears in the Chinese interface
  - After setting the time partition column in the table without data, an English error text appears in the Chinese interface
- Fix problems of web UI
  - The case of the word JOIN is inconsistent between the model created by SQL and the model created manually
  - Precision display is inaccurate when the measure return type is Decimal
  - When deleting jobs in batches, the number of jobs displayed in the second confirmation popup is wrong

#### Kyligence Enterprise 4.1.0 release note

**Product behavior changes**

- Remove Avatica JDBC dependency from query engine to optimize query execution process, which will lighten the query architecture
- As the product is in the early iteration state, many module changes are involved in the user guide, which causes additional maintenance costs, so the user guide will be offline for the time being

**Feature**

- Support multiple All nodes for simultaneous service, forming a highly available architecture and improving system stability
- Provide rollback tool
  - Support metadata and data rollback tool at system and project level which improves system fault tolerance
- Provide API for monitoring node availability of querying and building, which helps users obtain the status of clusters' core functions, and complete daily operation and maintenance more efficiently
- Support integration with Sentry on Cloudera CDH platform to improve enterprise level security
- Support for configuring different Kerberos accounts at project level, and provide more flexible and secure data access control at all granularities
- Improve segment lifecycle management
  - Support for discarding jobs
  - Support for discontinuous segment
    - Note: With discontinuous segments, queries can still hit models, but the data in the interval will not be returned when queried.
- Support model metadata import and export, so that users can easily migrate models in different environments
- Support max dimension combination when editing aggregate groups to improve index pruning capabilities
- Support for generating system and job diagnostic packages via the Web UI

**Enhancement**

- The metadata migration tool supports the migration of ACL information, which guarantees the integrity of privilege information when upgrading from Kyligence Enterprise 3 to Kyligence Enterprise 4
- Support for continuing to build indexes from where building fails, reducing the resource strain from retry
- Query history is migrated from InfluxDB to RDBMS metadata to avoid InfluxDB single point bottleneck.
  - Provide an environment checker `Checking Query History` to check whether the query history can be correctly written into the RDBMS metastore
- Disable refresh and delete operations for locked segment
- Optimize parameters in configuration files, including removing useless configuration items, improving readability, etc.
- Updating metadata at the end of a building job to avoid possible build failure
- Separate the query pushdown engine from other functional engines, to avoid the inability to automatically obtain the time partition column format and range when query pushdown is turned off
- Fix security vulnerabilities detected by the Snyk tool, reducing the number of vulnerabilities from 71 to 42. Specific reports are available in the issue link.
- Optimize Spark CBO information accuracy to avoid possible building failure caused by broadcast join
- Optimize suggestion generation behavior on opening partial match parameters `kylin.query.match-partial-inner-join-model`
  - When turned on the parameter, accelerating engine allows the query to match a partial match model to generate recommendations
- Optimize index building process and enhance corresponding authority management
  - When a segment has been loaded, the unbuilt index can be filled by building index
  - When the user permission is operation, he is not allowed to modify the build method
- Rest API
  - Provides an API for refreshing table cache
- Improve system management capacity
  - Supports monthly aggregated retention of historical indicator data in InfluxDB, reducing storage and making it easier to analyze and forecast
  - Support monthly statistics of model growth and resource expenditures in Grafana to facilitate resource management
- Improve log and diagnostic bag
  - Improve user data security
    - Strip user's real information in diagnostic package to ensure information security
  - Improve operational efficiency
    - Add query annotation information into the query log
    - Optimize that when generating diagnostic packages via the Web UI, the generating process may stack in the background
      - When the user closes the pop-up window,  the corresponding process will be interrupted and the half-finishedfile will be deleted
    - Optimize that when generating diagnostic packages via the Web UI, diagnostic package tasks may be indistinguishable
      - Add task trigger time to the progress bar when generating diagnostic packages
    - Optimize for unfriendly warning messages that may be generated in logs when users change passwords
    - The system will timely generate  `jstack.timed.log` log files in the `logs` directory to record and locate some threads condition
    - Add time information to `kylin.out` to facilitate analyzing by time period when error occurs
    - Add Spark event log which records Sparder query events to the diagnostic package, guaranteeing sufficient log information for problem diagnosis
    - Speculate error messages to display the name of the error model when the model was saved
- Improve product usability
  - Open upload licenses access to ensure a smooth login process
  - Provide project level configuration whether to expose the computed columns
    - When this configuration is enabled, the BI tool connected to Kyligence Enterprise can get the computed columns defined in the current project
  - Supports more flexible username, allowing alphanumeric, alphabetic and partial English characters in the username
  - Support case insensitive in username, project name and model name
  - Trim and refine error codes. Optimize logs and front-end tips when reporting errors, improving user operation and maintenance efficiency.
- Improve user experience
  - Optimize the display of model lists and enhance the structure of model information
  - Add bootstrap in acceleration page to help users understand the meaning of acceleration
  - Optimize pagination defaults and pagination spans
  - Optimize the query result area for spatial display and guide users to query
  - Provide user-friendly navigation when directly entering pages with no access rights
  - Remove table index name that needs to be filled in when adding the table index
  - Optimize some of the text to make it more standardized and condensed

**Bugfix**

- When upgrading from Kyligence Enterprise 3 to Kyligence Enterprise 4, query may fail if the return type of a computed column is different from its customized type
- Fix a possible compatibility problem with the v2 API for Kyligence Enterprise 4 and Kyligence Enterprise 3 to ensure that the v2 API behaves the same on both versions
- When there are multiple projects with different name suffixes in the metadata, backuping and restoring the metadata of one of the projects may cause the lost of metadata of other projects with that name prefix
  - For example, projects named `test`, `test_1`, `test_2`, backuping `test` project metadata and restoring,  `test_1`, `test_2` project may be lost
- In a read-write separation environment, useless parameters may cause the failure of getting `work_dir` during environment detection, so the startup may fail
- If `hive-site.xml` file is missing in`$KYLIN_HOME/spark/conf`, the system may fail to read Hive data source correctly when loading database
- On the FI C70 platform, there is a possibility that the missing `hive.metastore.warehouse.dir` may result in an environmental check error
- When MySQL is configured as a metastore, the metastore cannot be accessed after opening session sharing
- When a user is not logged in or does not have a license, an error may occur when accessing pages within Kyligence Enterprise via URL
- After creating a new project, jobs under the project may always be pending because the database connection pool is full
- Users who are not system administrators or project administrators cannot access the table structure through the JDBC interface
- If there is a Broken model under the project, the table and column information in the project cannot be obtained through the JDBC interface
- Querying TRIM() function may get incorrect results
- Scan count and scan bytes may return error in constant query
- Query history interface query ID cannot be fuzzy search
- When using Mysql as a metastore, filtering queries whose query object is model in the query history returns queries whose query object is pushdown
- When the row number of query result exceeds the threshold, an error may be reported on the interface, but it still shows success in query history
- The query timeout threshold may lose efficacy
- In cluster deployment mode, the query node starts without sparder-sql-context, which may cause the initial query to be slow
- In a read-write separation environment, setting the data source immediately after creating the project may fail, and you can set it successfully after waiting for ten seconds
- When there are both computed columns and measures in the query, recommendations related to computed columns will still appear after acceleration failure

- It may fail to operate jobs when they are fully selected
- Indexes with the same ID in different models are displayed as building state at the same time
- The diagnostic package contains `access.log` that is outside the selected time frame which may not influence problem diagnosis
- Columns with timestamp type have no milliseconds in the source table, and the sampled data will show milliseconds
- Fail to batch process segment on the query node
- Fail to restart the index building task after deleting one of multiple segments during the building process
- No error message when failing to reset password when there is no ADMIN user
- Fix security problems
  - Permission validation is not performed when using API
  - Query history is not validated, and ordinary users can see the query history of ADMIN users
  - When a system administrator adds a user, the request password is explicit
  - The SMTP protocol password for mail and the password for accessing mail in `kylin.properties` are explicit
  - Users with query permissions can view Spark task details via the analysis page
  - When the project administrator does not have table-level access, error occurs when entering the data source page

- Fix problems with computed columns
  - When configuration of whether to expose computed columns is opened, the computed columns cannot be displayed in the Tableau
  - Possible errors in the flat table in the case of nested computed columns
  - When multiple models under the same project use the same fact table and different computed columns are defined on those fact tables, SELECT * query may fail to be accelerated
  - If the types of referenced columns in the computed columns in the source table is modified, query cannot hit the corresponding index after the source table is reloaded
- Fix failed queries
  - If the query is like: with ? as (subquery) and if the subquery contains union all, pushdown may fail
  - When querying COUNT DISTINCT for a derived dimension, the query may fail
  - The query may fail if the Chinese identifier is used in pushdown
  - Constant query with TIMESTAMPDIFF function may fail
  - If a user does not have access to a column ending in D in a table and uses select * to query other columns ending in D in that table, the query may fail
  - When the time partition column type is `varchar` and the selected partition column format does not contain hours, minutes and seconds, while the partition column time setting contains hours, minutes and seconds, the query may not hit the model.
  - A query may not hit the index when the types of key columns in the model are inconsistent

- Fix inconsistent query results
  - Results of the query hitting model and  pushdown are inconsistent when querying for columns with the same name
  - Results of the query hitting model and  pushdown are inconsistent when querying a column of the decimal type
  - Some of the date function's query results in Kyligence Enterprise are not consistent with those in Hive
  - If aliases are not contained in double quotation, case sensitive aliases in the results of the query hitting model and  pushdown
  - After some data types hit the model, the accuracy returned is not consistent with the query pushdown
- Fix problems with diagnostic packages
  - Query node-generated diagnostic package missing `audit_log` and `metadata` folders
  - Diagnostic package generated via Web UI missing `diag.log` file
  - Diagnostic package may not be downloaded when Yarn cluster is abnormal
- Fix interface UI
  - When the task is in error, pause, run status, the diagnosis option is put away, which may cause the generation of the task diagnostic package to fail
  - Fail to filter date in dashboard page
  - When editing table row-level permissions, check the table, and the columns in the table are still in uncheck status
  - When the user switches the number of paging in the job list, it may not be possible to trigger the full selection operation.
  - The filter condition is not updated when switch to the user page after filtering users in user group
  - User list disappears when setting users within a user group and save
  - Fail to save the dimension description
  - When you add a new item to the model editing page, a repeated prompt pop-up will appear
  - The language of text and error message do not match with Chinese and English version
  - When the browser window is reduced, the description information of the data source page overlaps
  - Fix browser compatibility problems
    - Possible misplaced button display in Firefox
    - Possible page mis-scaling in IE 11 browser
    - Unable to download diagnostic package manually in IE 10 browser, misplaced page display may occur

#### Kyligence Enterprise 4.0.13 release note

**Product behavior change**

- When recommendation mode is turned off, index recommendation is completely disabled.

**Feature**

- Optimize the speed of queries that hit exact index or hit count distinct (precise)
- Metadata address checking is added to the formal license verifying, and a formal license can be applied by generating a fingerprint file.

**Enhancement**

- Adjust default resource configuration of Spark to make it more suitable for production scenario
- Support project-level garbage cleanup via CLI tools
- Provide multiple sets of index optimization strategies for adapting to different customer scenarios
- Provide interface to support customized logout logic when connecting to third-party user systems
- Add permission verification to the `hive.metastore.warehouse.dir` directory in check-env. At the same time, the databases and tables without permission will not be displayed when loading tables.
- Optimize probe query performance, such as select * limit 1
- Optimize the default location of the jar file generated by Spark, and change it to the working directory set by `kylin.env.hdfs-working-dir`
- Define the index source priority. When the customized index and the recommended index are the same index, the index source is displayed as Custom.
- Support to write schema information into the customized directory when querying, in order to avoid possible query failure caused by accidental deletion of the  `/tmp` directory
- Rest API
  - Report explicit error message for v2 APIs that are no longer maintained in historical versions. For specific support list, please refer to the manual.
  - Optimize some API call methods and return fields, and add return field descriptions for some APIs to help to understand related call logic
- Improve enterprise-level security control
  - Enhance data security management
    - When users do not have model access or lack row and column level permissions on the tables referenced in the model,  they will be prevented from editing or viewing models and related indexes.
    - Refine project administrator permissions, and add system-level parameters to control whether the project administrator can change user's table-level, row-level, and column-level permissions
    - Optimize the user's error message when querying on the query page
  - Support encryption of metastore and InfluxDB passwords
- Improve product usability
  - Optimize the product behavior when reloading the table. When index refreshing is triggered after reloading the table, the user can choose whether to build the index immediately.
  - Optimize the interaction of the project administrator authorizing users under the project
    - When the administrator grants new user/user group project access rights, the system can synchronize and update the list of items visible to the user faster.
  - Optimize the problem of losing the model being edited due to the expiration of the session, and ensure that users will not be forced to quit when they have permissions to edit models
  - Support to jump from task interface to Spark task interface through IP address
  - Support to submit some forms through Enter, including model renaming, model cloning, adding users/user groups, password reseting and editing roles
- Improve user interface
  - Optimize the display abnormality of the index treemap when the index data proportion gap is relatively large
  - Optimize the display when the library name is too long
  - Optimize the copywriting when creating a project, avoiding long copywriting that cannot be displayed completely
  - Optimize the refresh operation when adding tables, and the refresh button now appears as a resident button
  - Optimize model list display when the table is too long, in which case shadow will be used to show that the table has not been completely displayed
  - Optimize the display of the acceleration page when the internet speed is slow, and add loading effect in this case
  - Center the copywriting of the importing SQL pop-up window at the acceleration page
  - Unify the style of the filter button at the header of the table
  - Optimize copywriting at the acceleration page
  - Optimize case specifications in English interface

**Bugfix**

- In HDP 2.6, Kyligence Enterprise may fail to start due to jar package replacement
- After deleting `hadoop_conf` folder, it may not be possible to start Kyligence Enterprise using kylin.sh start
- Kyligence Enterprise may exit out of memory due to failed log creation on HDFS by HdfsLogAppender
- In FI 6.5.1 environment, Kyligence Enterprise may fail to start Spark service due to insufficient permissions
- When starting Spark service, the correct ResourceManager may fail to be connected
- Kyligence Cloud: Fix the problem of using query cluster resources in the first step of building
- After the metadata is backed up on the Query node, the corresponding metadata backup file directory under the All node is wrong
- When a user only has permission to a table under a certain project,  he can get the results of another table if he queries the table using placeholders through JDBC.
- InfluxDB password is clear text in kylin.log
- Fix the interface permission problem. When the user does not have the project permission, he can directly modify his table permissions under the project through interface
- Fix the problem that the system administrator could modify the role of the ADMIN user to a normal user
- When executing a complex query, the system did not immediately show the timeout error message
- After deleting the columns in the fact table in Hive, the lookup table of the same model cannot be reloaded
- *Cast* function cannot cast floating into integer
- Building and table sampling fails when there are columns in the table whose column names begin with a number and end with *d*
- Implicit query fails when using *week* function as computed column
- When query contains *union* clause, the acceleration may fail
- Fix case insensitivity with double quotes in alias
- When the model contains dozens of *count distinct* measures, and these measures use computed columns, the building process may be choppy
- If there are table names, field names, or aliases in Chinese and they are not quoted using double quotes, pushdown fails
- When the aggregate index dimension is empty, the table index and the aggregate index may be considered to have an inclusion relationship, and the aggregate index may not be recommended
- During Kyligence Enterprise back-end initialization when starting, it is already possible to log in on Web UI, which may result in an error
- The details page shows abnormally when entering Spark UI from Application master
- When the value passed through the storage quota API is less than 1 GB, the storage quota on the page is displayed as 0
- When there are no recommendations in the model, calling the suggestions API in bulk may report an NPE error
- Repair user interface bugs
  - When multiple models are expanded, the index treemap between models may be misplaced
  - Username may be partially displayed
  - The popup window in source page displays an exception when an abnormal value is entered in the table sampling data range
  - Check box is invalid when filtering a column on the row and column access list page
  - In the English version, when the license expires, the error message is Chinese
  - After restarting the build task, accessing the query page may report an error
  - The data filter condition loses efficacy when the model is saved
  - If the login password is incorrectly, the password can still be modified and users can log in successfully while the error popup still exists
  - QUERY user can submit advanced setting under the model
  - Spark UI icon is not displayed in job page
  - Searching for a single user in user group page does not take effect


#### Kyligence Enterprise 4.0.12 release note

**Enhancement**

- Add the confirmation dialog box in some important operations such as modification or deletion to avoid the possibility of misoperation.
- Improve product usability
  - Model recommendation number can be clicked to show the detailed recommendations directly
  - Support Job ID as the filter condition in job page.
  - Clicking the target subject in the jobs list can jump to the referenced model.
- Improve user interface
  - Optimize the length of time range box in the dashboard page.
  - Optimize the default height of table when editing model to remind users more clearly that tables can be expanded to view more information.
- Rest API
  - Provide new APIs for indexes building / model optimization / model validation

**Bugfix**

- After pausing a job in the job page, the corresponding process is not completely cleaned up
- When the user guide is running, if the text input box is activated, user guide may fail with keyboard input
- Charts are not updated synchronously when switching time range in the dashboard page
- When setting a Count Distinct measure with a parameter of Precisely, building indexes may fail if multiple columns are selected
- The job details cannot be scrolled with the scroll bar after the job details are expanded in the job page
- When the source table contains about 4000 columns, it takes a long time to response and results are displayed without pagination.
- In model editing page, if the measure contains computed column, it fails to choose other columns after changing the function type.



#### Kyligence Enterprise 4.0.11 release note

**Feature**

- Optimize Hadoop platform compatibility: Support Huawei FusionInsight 6.5.1

**Enhancement**

- Optimize the query functions:

- - Support a new measure collect_set(column)
  - Support unix_timestamp function

- Improve user experience

  - Optimize the interactions of filtering in the list

  - Add a GIF in model editing page to guide the modeling

- Safety and stability

  - Provide a CLI tool to clean up invalid files on HDFS

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
