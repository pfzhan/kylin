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


cat > build/CHANGELOG.md <<'EOL'
### Release History

#### Kyligence Enterprise 4.5.14.0 release note

**Enhancement**

- Support for 9 String functions pushdown to tiered storage
- Supports using 'limit' to limit whether detailed queries use tiered storage to optimize query performance when detailed queries return a large number of results
- Add relevant observation logs
- The download result file format behavior of asynchronous query is unified
- Optimize the fifth batch of error codes and error messages.
- Add error code into job callback API
- During garbage cleaning, the system enters read-only mode and the build service is unavailable, which is not user wants
- After startup, complete the loading of all metadata, and then open the service to accept external requests
- A switch is provided to prohibit the query node from forwarding requests, so as to avoid affecting the query service due to the instability of the job node
- The transaction lock is queued for a long time, resulting in recommendation timeout and concurrent construction tasks

**Bugfix**

- Model cannot accept AI recommend resulting from out of boundary array
- Computed Column metadata is written to Table metadata, resulting in query error
- After adding CC column as a measure in batch editing, model saving fails with an error
- Memory leak caused by model import
- After the shielding setting is enabled, an error is reported in the derived dimension query
- After the model is imported into tiered storage data, a new CH node is added to the project, and the model list API reports an error
- When there are special characters in the table, the replace function fails to query the index and is not routed to the query push-down engine
- When querying an unsupported SQL, an unknown error code pops up
- Long SQL + MySQL will cause an error to save the query history
- Fix high-risk vulnerabilities 'Authorization Bypass'
- The configuration item of ansi.enabled pushed down by CH aggregation cannot take effect
- Load the sample data in the cloud environment, select a table and click the data source page, it will display blank
- When creating metrics in batches, check them in batches after filtering, and redundant metrics appear
- After adding the same aggregation group as the base aggregation index, edit the model again to add a dimension, the same aggregation group index as before will be lost
- In API load segment, the value of the parameter build_all_sub_partitions lacks validation
- In API set partition column, the optional parameter table_partition_col is invalid and should be a required parameter
- FI-C90 read-write separation environment, do not modify the nameservice scheme, some storage type view tables, press the query and report an error
- Memory leak when backing up metadata
- Overloading the table and adding columns without modifying the model will lead to the failure of recommended optimization suggestions
- After the administrator adds query permission to an ordinary user, the administrator can view that the list of tables that the user can access is empty on the authorization page
- Metadata conflict in loading table interface
- When a query uses tiered storage, the query total scan count shows zero
- Refresh the segment API and optimize the project lock
- Long transactions may exist in the interface of loading tables and reloading tables
- When kylin.query.request-forward-enabled is configured as false, the interface operation behavior is abnormal
- The columns in the model are missing, resulting in the failure of reloading the table
- If the model quantity is empty, the construction task fails and an error is reported when saving the model
- An error occurs when saving the optimization suggestions generated by SQL modeling
- The offline build task is submitted to the yarn cluster mode, and the job on yarn will not be killed after discarding, resulting in a large amount of resources
- Call overloaded table interface


#### Kyligence Enterprise 4.5.13.0 release note

**Enhancement**

- Refactor query pushdown to tiered storage, adding support for 10+ pushdown functions
- Add tiered storage query statistics dashboard in Grafana
- Fix SQL copied from query history to automatically remove newlines and spaces
- KOptimize the content of query history preservation
- Provide a recoverable escape channel when the spark master service of the build job is abnormal
- Add Protection strategy of Query stability
- Estimate the resource consumption of spark before submitting the task
- Support for varchar comparison calculation in where condition to be pushed down to tiered storage
- (Beta)Second flat table supports columnar storage
- Add protection measures to upgrade script to avoid data deletion caused by the user executing the upgrade script in the old installation path

**Bugfix**

- Fixed high-risk vulnerability Directory Traversal
- When repairing an instance in an instance pair, if other instance pairs under the same project is abnormal, the data size of the tiered storage in the model list and segment list is incorrect
- Optimization suggestions for batches of computed columns with the same expression, the interface prompts that the number of indexes generated is inconsistent with the actual number of indexes generated
- When importing a model, the case of the expressions in the CC column with the same name is different, so the model cannot be imported
- For a specific query, no index optimization suggestions were generated
- The query page selects "limit 500" by default, and the query history sql does not display "limit 500"
- Two models with the same name refer to tables with the same name of the same structure in different libraries, allowing overriding imports between models
- When the query uses CubePriority to specify the model priority, the content of CubePriority cannot be seen in the query history
- JDBC dynamically passes parameters, hits the cached query, does not convert ? into actual values
- Configurations that prohibit overriding at the project level can be added at the project level
- Data recovery on ClickHouse instances fails when routing to non-epoch nodes
- The model sets multi-level partitions, the global dictionary is missing, the query results are incorrect
- The build job fails in resource detection step, but it can be restored after restarting the job
- Syntax error with ESCAPE in SIMILAR TO

#### Kyligence Enterprise 4.5.12.0 release note

**Enhancement**

- When HDFS enables ACL permission control, the tiered storage function is available
- Compatible with FI MRS302 platform
- Optimized the performance of obtaining project metadata and model obtaining related tables
- Provides prometheus metrics related to tiered storage queries for easy statistical analysis
- Error code optimization, which helps users understand the reason for operation failure
- Async_query API need to allow multiple characters as delimiter
- Support snapshots to specify partition values to refresh partitions
- Tiered storage supports Bool data type

**Bugfix**

- For a fused model that contains aggregate groups of fusion data and streaming data, editing the aggregate group of the fusion data again and deleting the measures in the aggregate group will cause the model to be broken
- When View File System is enabled, the core-site.xml parameter include href fails to switch paths
- Importing model fails when newline in computed column is '\r\n'
- Query error ArrayIndexOutOfBoundsException: -1
- When optimization suggestion recommends multiple computed columns with the same expression, they cannot be added to the model
- Due to insufficient resources, the build job fails and an error is reported
- The lack of jdbc jar package causes failure to load external data source tables
- When there is a locked base table index, the job is suspended when importing data to tiered storage, and the locked index data will stop being imported after recovery the job
- When the model build method is changed from incremental build to full build or from full build to incremental build, the tiered storage data is not deleted, resulting in an error in the step of loading to tiered storage when building again
- Unable to load data to tiered storage at project-level when incremental model has empty Segments
- When the tiered storage and Kerberos are enabled, the diagnostic package fails to be obtained, and it prompts that you need to enter the Kerberos account and password
- The parameter kylin.source.hive.add-backtick-to-hive-table-name=false is enabled, and an error is reported in data sampling
- The project is set to skip the step of resource detection. After the build task is completed, the system capacity does not display the amount of data
- When the asynchronous query result contains timestamp format, it is different from the synchronous query result
- Create a multi-level partition model, set the sub-partition column as the partition column of hive, and the type of the partition column is int, and build job reports an error
- FI environment, an error will be reported when clicking to jump to the Spark UI details page on Query page



#### Kyligence Enterprise 4.5.11.0 release note

**Enhancement**

- The information about the metadata database and table are encrypted in the cookie to ensure security
- Provides APIs for creating models, model online/offline
- Index data in tiered storage and index data in DFS have the same change behavior
- Display grouped node information when tiered storage high-availability deployment mode

**Bugfix**

- On the build job list page, you cannot search and filter based on the refresh snapshot task object name
- When the system manages snapshots, it is allowed to use the specified spark conf parameters for refreshing snapshots during building, to avoid building failures caused by insufficient resources
- Fix security vulnerability
- Expansion rate display error
- Model view uses the with as statement. When the query field is a subquery, the query reports an error
- Non-UTC time zone, Segment Pruning result is incorrect
- When repairing the Broken model, add base index is checked on the save page, but the base index is not generated
- Tiered storage does not support pushdown of queries that do not contain 'order by' but contain 'limit' statements to Clickhouse
- In the tiered storage high-availability deployment mode, when there is one abnormal node in multiple groups of nodes, the query may not be routed to the normal node, resulting in the inability to use the tiered storage to answer the query
- Unable to stop query on Azure China cloud environment
- For query that hits the model, when the query result set is too large and the spark task fails, the query should not pushdown any more
- SQL modeling, upload the sql text and then modify the sql statement, click the "Save and Continue" button, the metric of the new model created is the metric before the modification
- After the model state is restored from the Broken state to the Online state, the original optimization suggestions are displayed on the details page without data

#### Kyligence Enterprise 4.5.10.0 release note

**Enhancement**

- Support to use tiered storage when View File System is enabled on open source Hadoop v.2.7.2
- Add a measure description field to support synchronizing the comment of the source table field
- Hide or disable unnecessary features in integrated mode with LDAP
- Chinese text appears on the English prompt page of the excessive amount of license data
- Support to configure the parameter that skip the resource detection step
- When there are many small files in the partition table, the detection resource time of sampling table data job is reduced.
- When there are many tables and columns in the model, improve the freeze phenomenon when editing the model
- Add the system level parameter to support to enable or disable the Real-time function
- Support to delete the base table index when tiered storage is enabled
- When offset is used for query paging, the query can be pushed down to tiered storage
- When order by + limit is used for query sorting, the query can be pushed down to tiered storage
- For segment retention setting, the logic of model level and project level settings is different

**Bugfix**

- Fix security vulnerabilities
- Because kylin.out log files have no automatic cleaning strategy, the disk is full
- Incomplete description of how Hive transaction tables are used in the manual
- When querying fields of type char(n), the index query results are inconsistent with Hive query results
- Cannot hit the aggregate index when group by constant in SQL which contains join subquery
- Upgrade from 4.3 to 4.5, and Kerberos is enabled (parameter of ZooKeeper client is not configured), and the global dictionary fails to be build.
- When the nameservice in Hadoop's hdfs-site.xml file contains uppercase letters, the data loading to the tiered storage fails
- The sorting of the number of rows and the expansion rate in the model list don't take effect, and it is modified to remove the sorting function
- In LDAP integration mode, fixed issue of an error message displayed on the login page when you log in for the first time or restart
- SQL optimization fails to recommend the expected computed column when the dimension column on which the computed column depends contains a keyword
- In LDAP integration mode , two CN users cannot mapping to permission
- In LDAP integration mode , creating ADMIN user is not allowed
- After deleting the join column of the model dimension table in the source table, the model is broken, but the model cannot be saved after modifying the join column of the model
- When the model contains dimensions with the same column names in multiple tables, exporting the model and then importing fails
- When the partition column is of string type, the latest data range is incorrect
- The same database name in different databases in the kylin.metadata.url can not be recognized as a new database
- In LDAP integration mode, optimized the error message when using incorrect password to connect LDAP
- In LDAP integration mode, connecting LDAP by using incorrect username then restart service successful , error message has been optimized
- In the Huawei fusioninsight-C90 cluster read-write separation environment, the Nameservice scheme is not modified, and an error is reported when loading the view table
- The add project, Add user group button styles are changed to the same as the Add user button styles
- In LDAP integration mode, adding prompt message when login if LDAP service exception
- In LDAP integration mode, due to LDAP service exception, the front-end page is blank after the user cache is invalid
- In LDAP integration mode, Invalid users with the same name were synchronized to the user group list
- When the user with query permission uses the API of "delete query result file according to query_id", the corresponding result file can be deleted successfully
- When Using async query API and the select column in SQL containning parentheses, the query will report an error
- When the MySQL metabase is in cluster mode, it will report an error when entering the query history page at the query node of Kyligence Enterprise

#### Kyligence Enterprise 4.5.9 release note

**Enhancement**

- When there are many small files in the partition table, the time-consuming detection of resources is reduced.
- Supports high-availability deployment of tiered storage clusters
- Upgrade brand, update logo

**Bugfix**

-  Model cannot accept AI recommend resulting from out of boundary array
-  Querying the FLOOR function reports an error
-  Field of Decimal type in the Where statement is not supported to be pushed down to tiered storage for comparison
-  The ORDER BY statement is not supported to be pushed down to tiered storage for sorting
-  When the row level ACL setting API is used with input parameter of table name / column name / database name in the lowercase, an exception will occur during query
-  After sum (expression) is enabled, the result of union all query is incorrect
-  Time partition column does not support yyyymm format of type bigint
-  The partition column is of VARCHAR type. If the partition column is converted to DATE type in the filter condition of SQL statement, segment pruning error will appear
-  Incorrect installation description of load balancing "Tengine"
-  Incorrect installation description preconditions : and hive's DDL permission is not required
-  When the S3 space growinng in AWS environment , the command line tool RoutineTool cannot clean up temporary files normally
-  Edit the model, add a data filter condition and report an unknown error code
-  If the SQL statement with hint cannot hit the model, an error will be reported and the query cannot pushdown
-  There is a kafka table with the same name, and it can be selected when adding a hive table
-  On the streaming job page, when all items are selected, the filtering function of the "Object" filter box is not available
-  Project administrator cannot configure project-level parameters, prompting no permission
-  The query limit is FETCH NEXT XXX ROWS ONLY keywords cannot be optimized and recommended, but limit can be recommended
-  COLLECT_SET measure cannot be answered with model when configuration parameter kylin.query.use-tableindex-answer-non-raw-query=true
-  When adding Row ACL, after the operator of VARCHAR column is changed from 'LIKE' to 'IN', it cannot be changed back to 'LIKE'
-  When the metastore.sh restore command is followed by an empty directory, there is no double confirmation step, resulting in the metadata being blanked
-  Incorrect query result when fusion model hits logic of dimension as measure
-  When Kafka and Hive data sources have tables with the same name, the number of tables is displayed incorrectly
-  Turn on table-level data sampling, and all tables are sampled when loading some tables
-  On streaming job page, after selecting all items, the filter item in the "Object" column is missing
-  When recommending optimizations, if there is a SQL Hint, select the model with it
-  When recommending CC, the CC of the offline state model is not reus
-  Some completed query tasks are timed out in spark UI and cannot be cancelle
-  Failed to execute check-env.sh on MAPR-6.1 platform


#### Kyligence Enterprise 4.5.7 release note

**Enhancement**

- Add recommendation generation rules
- Asynchronous query export format can be parquet
- Support size function

**Bugfix**

- Reload table reports error after changing data column type
- After adding a model dimension, data cannot be loaded into tiered storage when building index
- The execution of scheduled tasks is too slow, including performing garbage cleaning and updating optimization suggestions
- The transaction is opened for capacity accounting, and the memory usage is too high
- Memory leak when backing up metadata
- Aggregate queries answered by detailed indexes cannot be converted into optimization suggestions
- The base table index did not trigger an update when the column type changed
- In the project setting, optimization recommended time range and recommended frequency are set to decimals or too large integers, the page will report an error after saving
- After the Epoch times out and recovers, the optimization proposal generation task is not restarted
- When a user is assigned to a user group or a user group is assigned to a user group, an error message is displayed
- When too many users are assigned to a user group, the number of users assigned to the user group is inconsistent with the number of users displayed on the page
- Failed to merge two warning segments.
- A pop-up wrong message is displayed after the upgrade
- When there are a large number of user groups and users, some interfaces have poor performance
- An error occurred when executing diag.sh
- The required parameter ＂sub_partition_values＂ is not verified in API "Delete Sub-Partition value corresponding index data"
- When the capacity exceeds the license limit, the hyperlink in the alarm text is hidden on the capacity charging page
- Pre-delete table API, the information returned by the interface is inaccurate
- For pure flow models and fusion models, save and build disabled
- During metadata recovery, if the number of connections to the PostgreSQL metadata database is full, metadata recovery fails but no failure prompt appears
- After deleting a dimension table from the model and reloading the data to the tiered storage, the size of the tiered storage data in the model list is not updated
- The model does not have settings to override, you can choose to override the settings when exporting the model
- After the model is switched between incremental loading data and full loading data, the data cannot be loaded to the tiered storage when building index
- Failed to create a project after logging in to Kyligence Enterprise with a long user name
- Hitting the tiered storage query, the date type column returns the wrong result
- The data in the tiered storage is not purged when purging all segments the model
- Recomendations change the model join relationship
- Build error when excluded table as fact table
- Hit the wrong query, the query status is success, and hit native
- Pause the build job twice, duration still increasing.
- After the real-time job switches the host, the task cannot be started
- In "yarn - cluster" mode can build successful, but test resources have abnormal error log
- Refresh must be clicked when the data source is first loaded
- The system reports an error when use sum(cast('varchar' as double)) in query
- When there are a large number of user groups and users, some interfaces have poor performance
- The snapshot management API does not verify that the parameter "partitions_to_build" is empty
- Reload table prompt information is inaccurate
- Flame diagram - information about download timeout is missing in the log
- When asynchronous query results contain special characters, the CSV file format of the query results is incorrect
- The result set is within 1 million, which is required to be returned within 10s, but now the return time is much longer than 10s
- The resources applied for automatic parameter adjustment are larger than the available resources of the cluster, and the build has been stuck waiting for resources
- The build task was interrupted when garbage cleaning was performed while the build task was in progress
- After restarting KE, the status of the build task changed from RUNNING to ERROR
- A model error prevents the entire model page from being displayed

#### Kyligence Enterprise 4.5.6 release note

**Enhancement**

- Support function CORR
- Support CDP and Apache Hive transaction table
- Support range partition table for TDH environment
- Adapt to big data cloud sso
- Support Transwarp TDH text, ORC partition table, partition transaction table data source
- Support "Transwarp TDH"
- History table model query allows partial matching of association relationships
- When an error is reported after retrying the build taskOptimize, abnormal swallowing behavior
- Update prometheus monitoring indicators and alarm rules document
- Public the prepare unload table and unload table API
- Under Huawei FusionInsight Platform with a read/write separation deployment
- Stability index development
- Remove the Beta logo of the history table
- Optimize the error code of the error report interface

**Bugfix**

- Some complex expressions cannot hit the model
- CORR、 SUM  and PERCENTILE_ Approx use CC column as parameter type verification, missing date  Timestamp, boolean type verification
- The model is in the BROKEN status, and it fails to reload a table that is not referenced by this model
- In the full loaded model created by SQL, the partitionDesc of the model is not null, so that the full segment cannot be generated when it is set to no partition column
- When model is broken, you cannot create a new model
- Shuffle partitions of push down calculation is incorrect. The execution of shuffle stage is very slow
- Support parameter configuration of Spark view cache
- Resume the suspended job of loading data to Tiered Storage, the job went wrong
- Delete tables containing ongoing tasks. Unknown error codes are reported after the task is stopped
- Count (1) when answering count (column), an error is reported, and the index is out of range
- Close the pop-up window during model import, and select model import again, prompting that the analysis failed
- After deleting the model, the tiered storage data is not cleaned up
- The information returned by the pre-reloaded Hive table API is inaccurate
- When querying 50 million data, the query history shows success, but the actual front-end interface is always loading
- After deleting the project, the job of the model is not deleted on yarn
- KYLIN_ When home ends with - query, the system diagnostic package fails to be printed
- After the index is built, the segment in the waring state can be refreshed normally
- Reload table prompt information is inaccurate
- After the snapshot is automatically generated, the metadata snapshot_last_modified value is not recorded
- The page of build task details read the same IP
- The index ID displayed in the query result is inconsistent with the ID displayed on the index details page
- Users can change permissions for themselves
- Cannot answer count() with tiered storage
- After the model is renamed, the model name on the streaming job page does not change

#### Kyligence Enterprise 4.5.5 release note

**Enhancement**
- Support CORR function
- Error messages disappear too quickly, the component behavior needs to be adjusted
- The default setting will close the function of building resource detection timeout
- The status of last level 2 build job update
- Remove the beta logo of intelligent recommendation function
- Correct the calculation logic of capacity billing function
- Files in Hadoop_conf are missing, build abnormal can't displayed
- Build the task monitoring page in real time, increase the granularity of observation   Ïtime range, and change it to drop-down
- Recommended interface timeout optimization
- Query statements execution error reports, and optimize with SQL logic
- When the query statistics switch is turned on, the CPU consumption of high concurrency query is too high, resulting in a large number of slow queries
- Obtain model information interface optimization requirements
- Supports simultaneous execution of multiple jobs of loading data to Tiered Storage under the same model
- Model list loading optimization
- Provide batch operation interfaces related to users (groups) and permissions
- Improve diagnostic query error ease of use - log error messages in query history
- Real time support for Kerberos environment
- Query protection mechanism, add SQL blacklist and error caching mechanism
- Check-env.sh capability is enhanced

**Bugfix**
- After turns on intelligent recommendation, it optimizes SQL and types diagnostic package. There is no job in the diagnostic package_ TMP directory
- Settings-Advanced Settings-Click to open the View Manual in the multi-level partition pop-up window. The link page displays an error
- There are serious log4j2 high-risk vulnerabilities
- After the model is built , add dimensions and measures, modify the aggregation and detail indexes, save the error, and prompt that the index id is duplicated
- The manual link in the optimization suggestion preferences cannot be accessed
- Pid file does not exist in root catelog, script kylin.sh fails to be restarted
- Hit the wrong query, the query status is success, and hit natvive
- Edit the fusion model, modify the dimensions and measures, modify the index and cause an error
- Pause the build job twice, duration still increasing
- Api of getting model information does not return table association information
- After build job resumes, duration is negative.
- Query response time is always 0
- Delete suspended tasks, a job state is missing from the English copy
- Update metadata is unlocked, so build job report an error
- Resume the suspended job of loading data to Tiered Storage, the job went wrong
- Status icon in batch data job is not attractive
- The build job contains build or refresh snapshot, and the metadata is not updated after the job is restarted
- Checking Spark Driver Host failed
- Asynchronous query failed
- Query the data that is not within the segment time range and can hit the model normally
- Build job was retried and the snapshot metadata was not updated
- The columns in the model are missing, resulting in the failure of reloading the table
- If the model quantity is empty, the construction task fails and an error is reported when saving the model
- There is a loophole in the logic to determine whether the column belongs to the original table, which makes some SQL unable to be optimized
- ThreadLocal was not cleaned up during query retry, resulting in the query error array out of bounds
- Changing the time partition column format when incremental build will turn off the tiered storage switch of the model
- Restart after real-time task stop failed
- Fail to build snapshot incrementally when partition column is timestamp data type
- Flame diagram - information about download timeout is missing in the log
- No error messages are given in the error message when building job
- In the job list, the snapshot object is blocked
- During the process of building a snapshot, the log in the job details cannot be downloaded
- When the sub-partition column of type is Boolean, an error is reported when detecting resources
- Cannot pause or terminate the job of loading data to Tiered Storage
- Through the flow model query, when the query time is not within the construction data time period, the prompt information is incorrect
- GBase data source, jdbc data source is not effective to the default database.
- After deleting a project, the running and pending yarn tasks under the project still occupy resources and remain
- The offline build task is submitted to the yarn cluster mode, and the job on yarn will not be killed after discarding, resulting in a large amount of resources
- Modify the model definition. If the new table relationship is left join, the  behavior is inconsistent with the behavior prompted in the interface after submission
- When build one segment, the job duration is equal to the sum of the duration of each level 1 task.
- After the model building task is completed, delete the model, batch data task, select the building task, and return an error message. There is a problem
- The PID file displays multiple process numbers and prints them in the log
- The result set is within 1 million, which is required to be returned within 10s, but now the return time is much longer than 10s
- When the spark executor applies for memory, the build class task cannot exceed the maximum memory that can be applied for by a single applicationmaster container
- SSO compatibility issues in specific cases
- The default parameter for obtaining fuzzy matching in the item-specified table should not be on, which is inconsistent with the manual
- The calculable column used by model view is not added to the dimension column, and the query will fail
- There is a problem with the SQL generated by the model view, which leads to the failure of SQL parsing and error reporting
- Enabling the mode view function causes the query performance to degrade
- There is inconsistency between model view query and ordinary model query
- CC metadata is written to table metadata, resulting in query error
- Suggestions generated using SQL modeling on the interface cannot be added to the index column table
- Memory leak caused by model import
- Use the yarn-cluster mode diagnostic package to obtain the eventlog log path is incorrect
- An error is reported when querying limit 10 offset 0


#### Kyligence Enterprise 4.5.4 release note

**Enhancement**
- Break down the job steps, improve the ease of diagnosis
- Query diagnostic package, improve the ease of diagnosis
- Query flame diagrams, assist in query problem diagnosis
- Support snapshots to specify partition values to refresh partitions
- SQL hint syntax compatible with  model priority
- Hadoop compatibility - support "Transwarp TDH"
- Real time function - log diagnostic package optimization
- Support the ability of TDS binding model
- Support to jump directly to the Spark UI address of the real-time task
- Optimize building performance through adaptive spanning tree and second flat table
- The type of partition column need be specified when Tiered Storage is being used
- When no user under the user group, it should not request again to fetch information from LDAP server
- Fix that when assigning users in groups with large number of users, it will freeze
- Call overloaded table interface
- Improve the product behavior of computable columns in intelligent recommendation
- Improve the sample.sh tool to provide sample data and model functions
- Record the number of queries of constants type in influxdb
- Remove tiered storage entrace from project settings
- Support count distinct case when else (cast null as XXX) usage
- The data source cannot add a new table
- Modify model name and interface requirements
- Obtain model information interface optimization requirements
- Provides a unified open API for modeling and index optimization
- The front-end interface displays the duration of generating query diagnostic package
- "Query history duration filter"fill in the left first, and it will be changed back to 10
- Azure environment, between flat table persistence and index building, temporary file cleaning optimization
- Verify real-time function - support read-write separation
- The real-time function flow model and fusion model support the regular refresh of dimension tables
- Tiered storage supports data rebalance

**Bugfix**
- Query node in Ha environment, stream data task reports sandbox error
- After the merge job is completed, the HDFS temporary index directory is not cleaned up
- Fix the problem that the KE service cannot be used after the abnormal metadata recovery
- The number of threads rises sharply, the Ke service is stuck, and a thread deadlock occurs
- Fix transaction timeout when saving model
- Round function query result is incorrect
- After the infer filter is enabled, the model joins the same dimension table multiple times and reports an error
- The maximum number of dimension combinations for a single aggregation group cannot be set when creating an aggregation group
- Occasional model building is completed, segment metadata is updated to a null value, resulting in a capacity billing error
- When obtaining cluster information, the back-end transaction has already ended, but the page is in execution for a long time and does not end
- The number of rows in table sampling statistics is occasionally zero, resulting in inaccurate capacity calculations
- The fusion model query cannot hit the snapshot
- Model page - display "unknown" when expansion rate is not calculated
- Occasionally count distinct measures map to wrong columns
- Data source SDK, case incompatible for view fields
- Batch flow heterogeneous - cannot query real-time data after creating fusion model and adding metric top_n
- Capacity billing - when snapshot is enabled, the dimension table capacity calculation result will occasionally be zero
- Optimization model recommendation strategy
- Fix intelligent recommendation duplicate index layoutid problem
- An error occurs when saving the optimization suggestions generated by SQL modeling
- Build a snapshot, when the partition column type is date, the build fails
- An error occurs when saving the optimization suggestions generated by SQL modeling
- GBase data source, failed to build a snapshot
- After the environment is upgraded - spark error is reported - operations such as query modeling cannot be performed
- The number of snapshot rows generated during model building is 0
- A memory leak occurred while copying spark sessions
- The view table reports an error during table sampling
- Real time model, running longrunning, oom error on driver side
- Real time model association dimension table, running longrunning, data intake task error reporting
- Fix the issue that the content of the step of loading data into tiered storage is empty in downloaded log
- On the parameter configuration page of the data intake task, click the plus sign, and the current limiting parameter will automatically change from - 1 to 1
- An error is reported when an ordinary user downloads a query diagnostic package
- Fix the issue that tiered storage doesn't support multi-partition model
- Executes diag.sh script and does not generate log files
- The description of segment online status manual is inconsistent with the actual situation
- The query diagnostic package spark was not_ Accurately filter the executor under the logs directory
- The executor log file for the build task is missing
- Overload table - prompt information needs to be optimized
- Gbase data source, the jar package is deleted after the upgrade
- After a major change occurs to the model, the change will be deleted, and a second confirmation will still be prompted when the model is saved
- When editing a table alias, the delete Table button still exists and can be clicked
- Query error, null pointer
- After adding CC column as a measure in batch editing, model saving fails with an error
- Cognos query with where condition ("name" = n'france ") reports an error
- Fix the issue that concurrent import and deletion of data from tiered storage without filtering and intercepting operations
- Under special circumstances, after merging Segments, the old Segments are not cleaned up
- SQL query error :org.apache.calcite.util.TimestampString cannot be cast to org.apache.calcite.util.DateString
- After the shielding setting is enabled, an error is reported in the derived dimension query
- Model import failed because of parameter 'is base cuboid always valid'
- Data source loading is slow
- Model import and export will fail when there are duplicate indexes in the model
- Failed to refresh data in special environment

#### Kyligence Enterprise 4.5.2 release note

**Enhancement**

- Query distributed cache
- Overall optimization of JDBC data source front end, , and supports GBase data source
- Supports 'model as view',data model to provide external services in the form of a wide table
- Support custom job info when submitting jobs through API
- When the number of indexes is large, the realizationchooser is slow
- Each query has identity authentication, which has a great impact on the performance of small queries
- Funnel analysis retention analysis bitmap function requirements
- Support adapting source data field case
- Remove the entrance of index suggestion for streaming model
- Optimize the steps to check resources during the build process
- Tableindex does not support CC columns
- 'Count distinct case when' disassemble into 'bitmap_uuid',if there is no aggregate index to answer ,rollback returns 'count distinct case when'
- Improved query 'exactly match'
- The query on the result set after 'join' cannot hit the aggregate index
- Support to jump directly to the Spark UI address of the real-time task

**Bugfix**

- Configuring async logging causes OOM
- Diagnostic package generation timeout
- If the derived dimension query conditions are met, the SQL object verification of the query derived dimension failed
- In cache does not support project level caching now
- Funnel analysis retention analysis bitmap function connection account table hit cube problem
- Repair real-time SQL modeling import SQL verification error
- When sum (expr) is enabled, the query result of sum (cast (case when)) is inaccurate
- Fix OOM caused by KE not releasing config after restoring large amount of metadata
- Optimize index recommendation behavior after CC is closed
- When the job was restarted, spark_args.json was not regenerated, resulting in switching the working directory, and the job could not take effect

#### Kyligence Enterprise 4.5.1 release note

**Enhancement**

- Optimize SQL modeling interface performance
- Job callback API supports https protocol
- Increase the number of records in the snapshot list page
- Allow spark view to lose precision when loading tables and queries
- Allow the computed column name to be the same as the column name of the dimension table
- Completion index API adds priority request parameters
- Inaccurate time-consuming segmentation of the query
- Hide "add base indexes" checkbox when using the function of building model from SQLs for streaming models
- The cast function supports string type conversion
- Build Job API to add "start time" and "end time" return values
- Enhanced build job API, abnormal return specific error information
- Disable Spark canary by default
- The number of scanned segments field is added to the query result Summary
- Eventlog is not included in the diagnostic package occasionally
- Print all the fields required to cache the key in the log
- Job API supports specifying Yarn queue
- The "Index List" API interface increases the table used by the index
- Add diagnosis package for streaming jobs
- Support to build/refresh a single index
- Index recommendation optimization: add optimization suggestions of left join relationship to colleague real table model
- Optimize the statistical efficiency of lookup tables when building models
- In fusion model, time partition column is taken from Hive table in patch model
- LDAP User fetching time is too long, causing users to be unable to log in to the user interface
- Optimize the log in the query process
- Iterative processing of query result sets to prevent OOM
- Optimize query Executor log

**Bugfix**

- too much small objects leads to OOM when use recommend suggestion for about 2000 models
- Streaming merge job throws exception when running
- Queries beyond the range of segments show that they hit the model, but they are not actually saved to the query history
- When the query hits the snapshot, the front end shows that it hits native
- Auditlog synchronization error, causing the epoch to be released
- Failed to use scientific notation in SQL
- query_histories changed when using jdbc or odbc to query data
- Failed to call some methods under broken fusion model
- The SPACE function reports an error when the column is used as input
- Out of memory when using recommend suggestions
- During the index building process, the model changed in relation, causing the build to fail
- Fix the problem that the Query instance cannot detect the Job instance
- During the indexing process, the dataflow failed to update and the building continued, resulting in the index number being empty after the construction was completed.
- The project is not bound to the resource group, and the error message is not clear when querying
- The system has been stuck occasionally after execution of the start command
- The pushdown query of a specific SQL is inconsistent with the Hive results

#### Kyligence Enterprise 4.5.0 release note
In this new version, Kyligence Enterprise not only supports real-time data analysis, but also supports fusion analysis of historical data and real-time data under a unified model and query portal. Under the broadening of the analysis scenario, the simple architecture reduces the operation and maintenance cost. At the same time, the introduction of Smart Tiered Storage functions can support flexible ad hoc query scenarios with random combinations of multiple dimensions, bringing users more possibilities for analysis and exploration based on massive data. In addition, Spark, which is the core component of Kyligence Enterprise has been greatly upgraded, which greatly improves the stability of building and query; Kyligence Enterprise supports the automatic creation and maintenance of base indexes, which increases the model's coverage of queries. 
**Smart Tiered Storage**
Kyligence Enterprise provides HDFS/Object Storage and Clickhouse serve as two tiers of storage, significantly improving the performance of ad-hoc analytics and detailed query by providing the Smart Tiered Storage capability with ClickHouse. With pre-calculation and AI augmented engine, it can cover more analysis scenarios, and help Enterprise to operate in a refined approach and support business decision making.
With Smart Tiered Storage, business users can still consume the Unified Semantic Layer using their preferred tool without the need of knowing the underlying computation and storage mechanism, which will significantly reduce the labor and time cost in the model, and obtain a unified analysis experience. 
**Kyligence Real-time**
The real-time nature of data analysis determines the timeliness of business decision-making. Taking immediate action at the first moment of an incident is a key factor in the success of business operations especially in use cases such as fraud detection, campaign monitoring. 
Kyligence Enterprise 4.5 helps businesses such as marketing, risk management who want to do real-time data monitoring or hybrid analysis of historical data and nowadays data by offering a minute-level latency real-time query engine using Spark structured streaming under the same Unified Semantic Model and query entrance. 
Unlike most of the alternative solutions available in the market, Kyligence provides one platform to serve online, offline, and hybrid analysis, so it hides the differences in the underlying data and achieves simplified and unified analysis for business. 
In contrast to a separate architecture for batch and real-time data, a hybrid architecture means reduced complexity in architecture, shortened development cycle, and lower operation and maintenance cost for IT.
**Upgrade of Spark version**
In Kyligence Enterprise 4.5, the core component Spark is upgraded to Spark 3, which supports multiple important new features, and users can use upper-layer functions without paying attention to the underlying details. In data skewed scenarios, the existence of skewed partitions may cause performance bottlenecks in the building task. 
Adaptive Query Execution (AQE) capability improves building stability and query efficiency by adaptively repartitioning skewed data. The dynamic partition pruning (DPP) capability can perform partition pruning based on the information obtained during query runtime to improve query performance. In addition, Spark 3 also fixes more than 3400 patches compared to the lower version of Spark, eliminating the manpower investment in developing a large number of patches. 
Most importantly, Spark 3 will be a stepping stone for Kyligence to get Kubernized and provide a private-cloud deployment offering in the future. 
**Base Index**
If the query pushdown is not allowed in the enterprise, queries that cannot be answered by Kyligence Enterprise will fail. At this time, increasing the query response rate of the platform can ensure service stability to a greater extent. In Kyligence Enterprise 4.5, the base indexes are automatically generated when the model is generated, including all the dimensions and measures in the model. As long as the time range of queries match that of the built indexes, the query that hits the model can hit the basic indexes. And the base indexes support automatic update as the model changes, reducing manual maintenance costs. For more details, please refer to the user manual.


#### Kyligence Enterprise 4.3.7 release note

**Enhancement**

- Cognos Data Module reports fails due to "defaultcatalog" string part in the query statement
- Queries from Cognos with FETCH FIRST ROW clause failed
- Support individual resource configuration for build/refresh snapshot jobs
- Partial model matching parameters (kylin.query.match-partial-inner-join-model) support project-level configuration
- Optimize the processing when data skew occurs in the detailed index shard by column

**Bugfix**

- There is an SQL parsing failure and a null pointer error
- The maximum number of threads during partitioned snapshot building is incorrect
- When there is a Broken model, API for returning the model list API will report an error
- fix the alerts when pushdown called by sub query using quotes and brackets
- After the segment is built, the last update time is displayed incorrectly
- Check-env failed under yarn-cluster mode
- The driver log of the building job is incomplete
- The partition column format was not checked when building the index
- In yarn-cluster mode, the dimension table is a view, and an error is reported when counting multiple dimension tables
- When calling the pre-reload API, if there are duplicate columns in the dimension table, the result is not returned correctly
- During the upgrade, spark/conf/spark-env.sh will be overwritten, resulting in the loss of yarn-cluster mode parameters
- The build log is too large which leads to node Out-Of-Memory(OOM)
- Agg Pushdown prevents Olapcontext from being further segmented
- The Cross Join query resulted in the generation of more than 1 million Tasks
- Spark's repeated refresh of the Driver-side token causes it to fail
- The AppMaster log of the task submitted to Yarn is abnormal
- In SQL modeling, when the expressions of the computed columns are the same, tables with different join relationships may be identified as the same model
- If the query result set is too small, "the number of query scan records is 0" in the query history
- When there are no locked indexes in Segments, the locked indexes still exist in the index list

#### Kyligence Enterprise 4.3.6 release note

**Enhancement**

- Support to configure whether audit_log is included in the diagnostic package
- Allow concurrent building jobs during multi-segment building
- Optimize Percentile build and query performance
- Increase the width of the index details pop-up window, so that the content can be displayed as fully as possible
- Optimize memory consumption after enabling recommendation mode

**Bugfix**

- The current_epoch_owner field ip address of the metadata table is not accurate
- If the Broken model and snapshot exist at the same time while reloading the table, the copywriting will not display correctly
- The query history shows the step time, and the returned result is a negative value
- After setting up Shardby, a single task may process a large number of files
- The parameter spark.scheduler.pool setting is invalid

#### Kyligence Enterprise 4.3.5 release note

**Enhancement**

- Add User-Agent information to the access_log file to help diagnose problems
- Long parsing time when optimizing a large number of union queries
- Canary restarts abnormal SparderContext
- Open Download Query History API
- After configuring read-write separation, add write_hadoop_conf information to the diagnostic package
- Add result correctness check to build step
- Supports setting partial tables without building flat tables, assisting many-to-many query scenario implementation
- Support string function repeat (string STR, int n)
- Supports parameter settings If the query cannot be answered by the index, it will fail directly without using the down function
- kylin.query.use-tableindex-answer-non-raw-query parameters support project-level and model-level rewrites
- When the table is overloaded, the table structure changes, and the corresponding snapshot will be displayed as a Broken state instead of being deleted directly
- increased to check the existence of krb5 files
- Enhance the implementation of priority when submitting tasks
- Add logs related to optimization suggestions in the diagnostic package
- Asynchronous queries use a separate spark queue to support project configuration, which supports specifying queues in request parameters
- Support setting whether to level the dimension table
- Asynchronous queries using separate Spark queues
- Optimizing session-store using jdbc mode may cause QPS to drop
- Supports pluggable SQL Transformer plug-in

**Bugfix**

- build global dictionary check turned off by default
- Batch operation of the global task list may cause system jam
- Chrome version 65 browser, the index operation bar may display misplaced
- Capacity calculation is too small when the flat table is not persisted during construction
- Recovery of backup metadata failed
- When the number of indexes in a single model exceeds 10,000, the response time of related operations is too long
- Abnormal behavior using both forcedToPushDown and forced_to_index
- Using mySQL metadata, asynchronous query write query history failed
- ${KYLIN_HOME}/conf does not place krb5.conf, check env kerberos check error is not clear
- No realization found for OLAPContext
- Loading flat table data exception
- In addition to admin permissions, users with other permissions submit asynchronous queries, which must now fail
- Send asynchronous query, no record in query history
- KDU cannot be updated immediately, effective immediately
- SparkJobTrace causes serious query performance degradation
- Configure the project-level configuration to root.default, the actual asynchronous query is still the system-level configuration
- After submitting the query API, the asynchronous query status is all failed
- Execute a content query exception with 'Free Selection-Pepsi (' in SQL
- Multi-level partition sub-score search function is invalid
- Asynchronous query query time increases significantly compared to synchronous query time
- The query history configuration was successful, but the wrong metadata path was written during execution
- Recovery of backup metadata failed
- The query history specified metadata url path switch is not configured to record the query history failure, resulting in the inevitable failure of the next query
- Export query history data is too large, the export file is empty
- hdfs stores the path of the query job, the log is the same level as the job_tmp directory, and it is not convenient to read the log
- Union of column dimension columns and metric dependency columns that can be set as detail indexes
- Fix return task list API status parameter does not take effect
- Fix return task list API return non-project data
- Union of column dimension columns and metric dependency columns that can be set as detail indexes
- Create project error in FI-C90 environment "The system is trying to restore service. Please try again later"
- When the number of indexes reaches 10,000, optimize the operation time to within 5s
- When performing two consecutive filtering operations, if the response speed of the first operation is slow, the filtering results will lag
- The Get Model Information API gets dimensions only from the aggregation group
- Built indexes were not rebuilt after the model join relationship was changed to many-to-many
- When the model goes offline, the computable columns of the same expression do not correctly reuse the existing computable columns, causing the recommendation to pass
- PI operation error in query
- does not support index building for a single COUNT_ALL metric
- English interface selection dimension table does not play normally, pop-up window prompts Chinese
- Using query API forced pushdown, it report an error
- Model optimization suggestion sort only takes effect in the current pagination, not globally
- There is a problem with the download link of the jar file package driven by the manual JDBC
- Sort operations in model optimization recommendations only take effect for data within a page
- build global dictionary check turned off by default
- model segment list default sorting by chronological order
- After capacity billing is turned off, irrelevant values are still counted during snapshot construction, resulting in too long build time
- Inquiry Hit Index Turn Spark Failed to Error "Unsupported function name PERCENTILE_APPROX" When Executing Plan
- Get task information API input error jobid parameter, can also return the last jobid execution status of the project execution
- Capacity calculation is too small when the flat table is not persisted during construction
- Asynchronous Query-The front end displays "Number of Query Result Records" always 0

#### Kyligence Enterprise 4.3.3 release note

**Enhancement**

- Compatible with MapR 6.1.0 platform
- Compatible with Apache Hadoop 2.7.2 platform
- Support Count Distinct Case When Expression
- Enhance RLS with more control over AND/OR pairing
- Multi-level partitioning loading data API, supports specifying all sub-partitions
- The query result memory protection parameter is enabled by default
- Support zip compress storage for metadata
- On query history page, support normal users to filter query objects
- Print more cache-related information in the log to facilitate problem diagnosis

**Bugfix**

- In column-level permission interface, user name field is case-sensitive
- Not include the column with invalid data type in col-level ACL configuration
- Fix invalid data ACL after table reload
- The index overview entry and the number of rows in the Segment list are inconsistent
- Model import fails when there are many computed columns
- When more than two subqueries participate in UNION, the query may fail if any of the subqueries filter is always false
- System-level configuration of smart recommendation mode is ineffective
- By setting "kap.metadata.semi-automatic-mode=true" in the custom project configuration, you can turn on the recommendation mode
- When the column name contains double underscores, the query cannot hit indexes
- When the Spark Context stops unexpectedly, the query cannot be cancelled in time
- For the failed pushdown SQL, the query object is displayed as Hive in the query history
- The query history is not displayed correctly which hit the cached
- For unaccelerated SQL, the status returned by the query history interface is wrong
- The deleted model can still be clicked in query history
- When the query hits the snapshot, the index is still displayed as hit in the query history
- In asynchronous query to get query status API, missing the required project parameter, unknown error code occured


#### Kyligence Enterprise 4.3.2 release note

**Enhancement**

- Support to export query history
- Support to specify all sub-partitions when loading data in multi-level partitioning
- Segments pruning according to the dimension range when querying
- Export model JSON file with pretty format
- Provide a instruction about network port dependency for product components
- Optimize model cloning time when there are many computed columns
- Support to exclude tables serving AS-IS scenarios when generating recommendations to prevent inconsistent query results after derived dimensions are recommended to the index
- Support hiding sensitive information in diagnostic package
- Limit username max length to 180

**Bugfix**

- LDAP user can't log in when using alias name
- In Kerberos environment,if there is no krb5.conf file in the etc directory,the building is abnormal
- The job steps' waiting time displayes when it reaches the Running status
- The External catalog configuration name is misspelled
- In set the time partition column API, it report an error when setting the time partition column in the normal model
- Fix modal import issue when exporting modle with no recommendation but selected export include recommendation
- When a computed column is set as a Shardby column, deleting the computed column in the model will make that the tables cannot be reloaded
- When the index is not built, the copywriting shows errors after modifying the index
- The sum of the waiting time of each step of the job is not equal to the total waiting time
- In the prepare reload table api, when the table field is empty, the error message is not friendly
- Optimization tips after login when user doesn't have permission to create new project and also no project accessible


#### Kyligence Enterprise 4.3.1 release note

**Enhancement**

- Project-level Kerberos configuration, the table permissions are not scanned by default, the table permissions are checked when the table is loaded
- Increase the CPU utilization of query nodes. When pressure test is token in a laboratory environment, the utilization has increased from 60% to 80%.
- Optimize the display of the time bar when the time of query substep is too short
- Optimize the prompts when switching to other pages as performing queries on the analysis page

**Bugfix**

- When multiple instances have the same working directory, concurrent execution of check-env fails
- Email verification prompt did not change when switching between Chinese and English
- Can't set Project ACL for a group which has the same name of an Admin user
- The maximum supported number displayed on the SQL modeling page does not match the back-end configuration
- In multi-level partitioning models, when the sub-partition value exceeds 2000, it cannot be displayed correctly
- Storage monitoring metrics always access HDFS, resulting in waste of TCO
- Query page hover display frame is too long

#### Kyligence Enterprise 4.3.0 release note
Kyligence Enterprise 4.3 provides detailed and efficient data management functionalities.  The introduction of Multi-level Partitioning provides business the ability to process data from different regions at different time. Snapshot Management enables users to control and utilize resources better. 

Resource Group is designed to enable business units to isolate data access, build and query requests. Model Migration supports the incremental update of a data model.  Asynchronous Query allows users to export large data set efficiently. 

Kyligence Enterprise 4.3 is fully integrated with Tableau. Export TDS makes it easierier for users to synchronize data model to Tableau.  There’s no repetitive modeling across Kyligence and Tableua anymore .  Fast Visualization have achieved new levels of speed for business to visulize the query results.

Detailed and Efficient Data Management
Multi-level Partitioning

For business that operates across regions, the time to update data can be different. Kyligence Enterprise supports up to 2 level partition on demand for independent data analysis by region/defined BUs. Data can be loaded and managed based on time zones and regions separately.

Snapshot Management

When the size of the dimension table is increased, Kyligence Enterprise supports the building of new snapshots on demand to improve the efficiency of model building. When the source table is a partitioned table, incremental update of the partition snapshots is also supported to reduce the time needed to build large-dimensional tables.

Enterprise-level Capability Improvements
Resource Group

Kyligence Enterprise provides business the flexibility to isolate or share resource groups​. Thus improves stability​.  The innovative design of this feature allows business to operate independently yet to share resource group and improve the utlization of resources.  Based on different user scenario, it is flexible for exclusive or shared resource groups by project​.

Model Migration

Model migration is improved to support the incremental update of model.  This feature ensures the model consistancy when migrating to different environments, backing up for disaster recovery and version control.  Model is exported as metadata file.  The UI based or open APIs make the migration smooth and easy. 

Asynchronous Query

Asynchronous execution of SQL queries provide a more efficient way to export data. When the result set of the query is too large or the query execution takes too long, the asynchronous query can efficiently export the query result set, helping to expand various application scenarios such as self-service data fetching.

User Experience Optimization
Export TDS

Models can be exported as a TDS file. After importing into Tableau, user can directly synchronize the tables, association relationships, dimensional measures, and other information in the model.  This easy to use feature reduces repetitive modeling work in data analysis,  and improve analysis efficiency.

Fast Visualization 

When the query result is displayed in a table, the information display is direct but not intuitive, and fast insight cannot be achieved. Kyligence Enterprise provides fast visualization of query results where users can view query results in intuitive way and discover valuable data faster.

Supported Hadoop Distributions
Compatible Hadoop Distributions:

Cloudera CDH 5.8 / 6.1 / 6.2 / 6.3

Hortonworks HDP 2.4

Huawei FusionInsight C70 / 6.5.1



#### Kyligence Enterprise 4.2.8 release note

**Enhancement**

- The ShardBy column can be used for optimizing join on column with high cardinality. For more information, please refer to User Manual
- In JDBC Preparedstatment query, replace the "?" placeholder with the actual parameter value in the query history.
- When modifying the model loading mode or partition settings, add a prompt to clear the Segment
- In the query history, show whether the index has been deleted, and increase the readability of the hitted index information

**Bugfix**
- When a calculated column is recommended as a dimension, if the name of the dimension is consistent with the name of the column, the recommendation will fail to be accepted
- Recommendations  that are invalid due to garbage cleaning are still displayed on the interface
- Fix the issue that modifying the source table or the model causes the deleted index to reappear, and supports whether to regenerate the deleted index when editing the aggregation group
- In cluster mode, delete and recreate the same name project, query node will apply old metadata
- Project permission is kept when project deleted in HA mode
- Capacity chart should show selected X axis (timeline) even no data in current time period
- When editing the table index, the column order after selecting all and saving is different from the editing page
- The input box of the setting page is too narrow, which causes the possible incomplete display of input content
- Occasional sampling jobs show errors, but actually succeed
- Add interception when adding duplicate groupby columns in TopN measure 
- Asynchronous query API does not verify project permissions
- Asynchronous query API, delete the result file according to the time, the time format is incorrect, an unknown error code is reported
- Asynchronous query API, after submitting the query, immediately call the return query status API failed
- Occasionally abnormal global dictionary building results in abnormal query results
- The query that hits the snapshot is not cached normally

#### Kyligence Enterprise 4.2.7 release note

**Enhancement**

- Optimize the loading speed of the snapshot management page when the number of snapshots is large
- When the system judges that the node memory required by the job can never be satisfied, terminate the job instead of waiting forever
- Improve the use of self-service tuning, when viewing the query object in the query history, you can choose to view only the index hit by the query to help index positioning
- Add the like_rows parameter to the row and column level permission API to support the filter condition of ‘like’
- Add new time partition format yyyy-MM-ddTHH:mm:ss.SSSZ
- In asynchronous query API GET DELETE method, project parameter change to URL parameter

**Bugfix**

- When the snapshot is built in partitions, if the different data in the partition column only differ in case, the uppercase part of the data may be lost
- Add row-level permissions, after adding like multiple times, the dividing line will overlap with the title
- On query history page, when there are too many filter results, the filter box is too long
- On query history page, hover the mouse over the SQL, when the SQL is too long, it will exceed the display box
- After deleting the dimension which is referred to by recommendations in the model, the total number of recommendations is not updated
- When the partitioned snapshot table has more than 32 partitions, it cannot be hit by the query after the building is completed
- When entering the maintenance mode, the hint does not appear when creating models
- Occasionally the same substep is displayed twice after the query is submitted
- In asynchronous query, when the user has no project authority, the error message is inconsistent with the normal query
- Char type data desensitization value is not displayed for accuracy
- After entering the maintenance mode, user can still modify the recommendation rules
- Improve the operation of recovering metadata files
- The execution in maintenance mode can not be interrupted
- The user name is case sensitive when the user is granted access to the project
- When creating recommendations through SQL modeling, there is a very small chance that the measures may be out of order, resulting in the failure of creating recommendations through query history.
- When using SQL modeling, automatic model naming does not consider offline models, resulting in failure to save
- Optimize the segment status, when a building job is running under a segment, it will be displayed as Loading status
- Use view table for modeling, when the order of the columns in the view table changes, the recommendations may fail to be accepted after reloading the table
- Duplicate references to the same index cause performance degradation
- When deleting items in batch, the metadata is not deleted cleanly
- When modifying column types and deleting columns in the same source table at the same time, the building jobs may fail when reloading the table
- When smart recommendation is enabled and there are too many models in the project, optimize the loading speed of the model page
- There are timeout queries not terminated in time
- A count_constant measure will be created in a model by default, and extra count_constant measure cannot be added manually, in case the model fails to be saved
- The occasional query result is empty due to concurrency
- When parquet.filter.columnindex.enabled is true, the query result is inconsistent with the pushdown query


#### Kyligence Enterprise 4.2.6 release note

**Enhancement**

- Public API for getting Index list
- Public query history API
- Query API, add whether it is a return item of partial hit segment
- Building jobs support yarn-cluster mode execution
- Support ESCAPE syntax
- Optimize the performance of some SQL issued by PowerBI
- For some interfaces containing the project name in the request path, correct the forwarding logic
- Improve the convenience of setting up fact tables when creating models
- ADMIN user project-level authority, set table-level authority restriction, the interface shows that the table access authority is correct, but the query or operation authority is not restricted
- Columns in loading more  can’t be automatically predicted during query input
- Optimize the home page experience, add explanations for less clear concepts and more
- Optimize the license capacity return code when exceeding the capacity
- When modifying the type of the column used as the join key, partition column, and model filter condition, since it has no effect on the model itself, the table will not be BROKEN after reloading
- The cell length in the query result is too long
- The health API is changed from synchronous to asynchronous to prevent failure by timeout
- When the stored data is too small, optimize the expansion rate display to prevent interference with normal use
- Optimize the display of cardinal products of joint dimensions, and use number formatting to enhance readability

**Bugfix**

- When upgrading from version 4.1 to 4.2, the environment is unavailable due to limited license capacity
- Inconsistent metadata causes the node epoch to be released
- Query history API, it cannot be called for some projects
- Jobs in the running state will report an error when restarting them
- The job status is discard. The discard api is called on the job, and the call is still successful
- The job status is pending, use api to restart this task, the call is still successful
- Management user, without model table permissions, but can set mapping rules
- Manual garbage cleanup, no metadata cleanup of recommended tables
- The project name and model name in the completion index interface are case sensitive
- Among multiple segments, when one of the segment indexes is full, if the completion index interface is executed, the jobid is returned, and the index building job is executed
- During the full build, if there is already a building job, modify the aggregate index and click Build to save. At this time, no new job will be generated and there will be no prompt
- Use the yarn-cluster mode to submit the building job, and the model that contains count distinct measures fails to build
- Use yarn-cluster mode to submit the build job, and the relevant logs are missing in the diagnosis package
- Build Snapshot optimization, change the normal column of the source table to the partition column, the refresh snapshot job fails after modifying the partition column
- Building Snapshot optimization, when  deleting the column and then getting the partition column, an unknown error code is displayed
- Building snapshot optimization, the project admin user has set no permissions for a certain table, and this table can still be seen on the snapshot page
- Building snapshot optimization, set partition column API, users without table-level permissions can still be called successfully
- Building snapshot optimization, set partition column API, query user should not have permission to call the interface
- After the SQL with response timeout was manually stopped, the corresponding Spark job was not cancelled, resulting in slow subsequent queries
- Building snapshot optimization, when adding a snapshot, the selected table does not appear in the partition column setting, but the selected table snapshot is refreshed
- Building Snapshot optimization, editing partition column report error
- Interactive optimization on the new homepage. When there is no model available, click one key to accelerate, and click the close button in the upper right corner of the pop-up window, it jumps to the model page
- Interactive optimization of the new homepage, the first time you click the query history "go to view", the filter results are inconsistent with the filter conditions
- Interactive optimization on new homepage, lack of information prompts after querying historical copy
- The query result page is inconsistent with the design
- On model Segments list page, with more blank space displayed between storage size and operation
- The multi-active job nodes function, the service status is displayed incorrectly, and there is no text prompt
- Job operation API behavior is inconsistent with the front end

#### Kyligence Enterprise 4.2.5 release note

**Enhancement**

- Support encryption between Spark cluster nodes, please refer to the user manual for more details
- Support visualization of query results, please refer to the user manual for more details
- Support the visualization of the query execution steps and duration to help locate and optimize the slow query, please refer to the user manual for more details
- The cartesian generated by a single query can easily lead to the instability of query nodes, which supports breaker for protection, please refer to the user manual for more details
- Add the index ID corresponding to the recommendations in the response of Approve Model Recommendation in batch API, please refer to the user manual for more details
- Optimize queries that contain a large number of unions
- Provide information about recommendations in the diagnostic package to improve operation and maintenance capabilities, please refer to the user manual for more details
- In multi-active job nodes, optimize epoch update mechanism, to prevent the overdue of partial projects
- Improve the generation speed of diagnostic packages when infuxDB is not configured
- Add log for timing in fetch_file_status
- Optimize query results to be 20 times larger than the original file size

**Bugfix**

- When using COUNT_DISTINCT, TOPN, PERCENTTILE_APPROX metrics, the column type is incorrectly used as the function return type, causing the query to fail
- When calling the Approve Model Recommendation in batch API, when the same table is used two times in the same model, or there are columns with the same name in different tables, the calling fails
- Correct the kylin.storage.columnar.spark-conf.spark.yarn.am.memory configuration name in the kylin.properties configuration file
- When refreshing the segment, delete the index under the segment, the job is discarded, the job details is still in the running state
- The placeholder copywriting for setting partition column type  is incorrect
- After adding duplicate TOPN measures, the error message is not obvious
- When upgrading to version 4.2.5, if you have logged in as the Admin user before the upgrade, and the upgrade is performed in a very short time, the upgrade will occasionally fail. The upgrade from version 4.2.5 will not have this problem
- Optimizing English copywriting of importing model function
- Optimize the system's automatic naming rules of the dimensions of the same name to prevent the use flow from being affected
- Occasionally the status of the job is inconsistent with the sub-step status
- The total number of user groups obtained in the user group management page is incorrect
- The project administrator cannot modify the User Rule in the Recommendation Settings
- When importing SQL for modeling, if a column is not in the table loaded into the system, there is an error in the error copywriting
- Unsampled and no snapshot dimension tables are not normally calculated into the data volume
- When initializing the Admin user, if the random password is not enabled, the login may fail
- Black dots obscured texts when hovering in IE browser
- When importing the model in IE browser, the interface display is misplaced
- Some special characters are not supported when searched on the model edit page.
- When batch adding dimensions, the cardinality value alignment of the fact table and the dimension table are inconsistent
- When adding a duplicate table index, the error message is not clear

#### Kyligence Enterprise 4.2.4 release note

**Enhancement**

- Support SQL Hint to specify model priority in a query
- Optimize the default configuration of thread pools that match Olapcontext concurrently
- Support deriving the partition conditions on dimension tables when the relationships among tables contain the partition column, which will reduce the scanned partitions and building cost
- Compatible with the new version of Tableau SQL syntax problem (left join-> inner join) to ensure queries can be answered by index
- Optimize the model matching logic for sum ( cast( then column else column)) query
  **Usability**
- Support viewing data type, cardinality, and other information in the query page
- Optimize text related to empty segments to improve comprehensibility
- Optimize keyword description of the search box on the data source page
- Provide the service period expiration reminder of the formal license
- Improve user interaction when computed columns whose type are Varchar are not supported in measures
  **Automated Integration**
- Support exporting TDS through Rest APIs
- Support building the lacking indexes into selected segments
- Support prioritizing jobs when submitting through Rest APIs

**Bugfix**

- After column-level permissions are defined, the query fails when the corresponding column is used for aggregating query
- If you upgrade to version 4.2.2.3044 ~ 4.2.3.3048 from a historical version, recommendations are not visible on the model page
- When an unauthorized user calls the API for approving model recommendation in batch, the request succeeds
- When the model name contains uppercase letters, the Approve Model Recommendations in Batch API does not take effect if the parameter filter_by_models is set to true
- The process of generating recommendations by SQL modeling may fail if there exist indexes that can be merged in the same batch of generated recommendations, and the indexes can be reused
- When the dimensions in the two aggregated groups differ only in order, there will be multiple indexes with dimensions in a different order, and such indexes cannot be deleted or built
- When a query does not contain any dimensions, SQL modeling fails
- The mail notification feature on the project setting page doesn’t take effect.
- The Kyligence service status cannot be returned in time when the ZooKeeper environment is unstable.
- The Job node does not support queries, but the query page still shows the query running process
- Leading and trailing whitespaces from the configurations are not trimmed, which may cause the parameter not to take effect
- The building job will be retried by considering the shortage of resources if the Spark application is killed by YARN, which may cause the job will not be switched to an error status.
- When defining model dimensions, the unselected columns will also be checked for the name duplication
- The refreshing segments status is not updated as expected when deleted the model indexes
- The model containing the Warning state Segment cannot be set offline
- When a user group is created, the user group of the same name is checked with case sensitive
- In IE11 browser, the button on the index overview page cannot be displayed completely
- After clearing the indexes in the Segment with the warning status, the Segment remains in warning status
- When upgrading Kyligence, the status of the Grafana application is not checked and thrown error exceptions

#### Kyligence Enterprise 4.2.3 release note

**Enhancement**

- The query history is saved for up to 30 days and 1,000,000 entries by default
- Make index and snapshot usage times independent of smart recommendation
- The floor function does not support day and week parameters when querying
- Remove the extra metadata of the snapshot in the Segment
- Support NVL function, no need to manually open the configuration
- Task monitoring page, optimize the error message of illegal state switching

**Bug**

- User groups with spaces at the beginning and end of the interface can be called
- The computed column cannot be selected as the GROUP BY when selecting the GROUP BY column of the TopN measure column in TopN measure
- dump.hprof file will only be created during the first OOM, which affects diagnosis
- On Task details page, English is still displayed in Chinese mode
- On Data source and analysis page, single table on top, leading to database collapse
- The /api/system/backup interface does not have verification permission
- After failing to detect the Spark environment, the document path prompted is the historical version
- Updating snapshot of build task  takes too long
- When columns are added to the source table and reload it, the deleted custom indexes will be restored
- The query actually hits the index and snapshot, but only the index is shown in the log Summary
- In the query editor, the clear button is inconsistent with the text in the pop-up box
- When creating a project, it prompts "The added project is in AI augmented mode", but the actual project does not start smart recommendation by default
- When creating a joint dimension, an extra prompt appears
- When the service status is not obtained, the node information at the top is lost
- The duration of query history should be limited in size range, and the end time should be greater than the start time
- The system capacity node list is displayed abnormally in IE 11 browser
- Using IE11 browser, the screen flickers when exporting metadata
- The log will print Create admin user finished.
- In the query editor, the clear button is inconsistent with the text in the pop-up box
- In the English interface, the amount of used data in the dashboard is not fully displayed
- Using Project ADMIN authority user, editing table row and column level authority do not take effect
- The SQL statement that hits the model, due to insufficient environmental resources, changed to push down
- When loading the data source, click the Enter key and proceed to the next step. The data source is added abnormally
- After using the upgrade tool to upgrade KE3 metadata to KE4, the model status in the ready state is warning and the expectation is offline
- When the error message contains <>, it will be converted into an HTML tag and returned incomplete error messages
- substring supports functions as parameter input
- When enabling SUM(expression), it may fail to parse SQL and generate the model
- Tasks in the Pending state, the kylin.log log cannot be obtained in the task diagnosis package
- In the read-write separation environment, the flat table is stored in the write cluster
- Query via API, Limit is not effective
- When no user/user group is selected, you can still click the submit button to submit successfully
- The "original_size" in the metadata table_exd and the "ori_snapshot_size" in the data flow are not deleted after the garbage cleanup
- When editing an aggregate group, there is no need to automatically add COUNT_ALL measure, if count(constant) already exists
- OBIEE BIP generates case when SQL with dynamic parameter, query error
- Revise the status definition and status processing of the maintenance mode
- The diagnostic package command line uses the includeMeta parameter to report an error
- KE4.1 upgrade to KE4.2, the upgrade will fail if session sharing is not configured
- Add interface for batch modification of user project permissions
- On the single instance mode, after restarting, push down and report an error
- When using Tableau to open the tds file exported by the model, the data type of some of the measures corresponding to the column is invalid
- In the asynchronous query API, , submit query should not return MISSING status
- The interface information is not refreshed immediately after the license is updated
- The executeAs parameter does not take effect on the data mask and column-level access control for associated row values
- Hit the snapshot of the model many times during query, but the query history is not displayed normally

#### Kyligence Enterprise 4.2.2 release note

**Enhancement**

- Support to manage snapshots manually
  - Support the same table to be used as a fact table and a dimension table in different models
    - Please note that when querying a dimension table separately (that is, a typical query scenario where a query can hit both the fact table and the dimension table), the snapshot is preferred to be used to answer the query
  - When the snapshot management is turned on, the system will no longer automatically build, refresh, and delete snapshots
- Optimize homepage display, graphically display intelligent recommendation related statistics, and support to optimize history queries instantly on homepage
- Optimize the SQL generated from Tableau LOD expression, providing configuration (false by default) so that specific SQLs can hit the aggregate index
- Support more optional parameters in model suggestion API to enrich the integration ability. For more details
- Supports query SQL with dimension or measure aliases longer than 128 characters
- Support async query with custom encoding format, file format, and file name
- If the query is divided into many OlapContexts, the query time may be longer
- When calling the model optimization API, add a new parameter to confirm whether SQLs are directly converted to indexes
- Optimize copywriting when adding empty segments

**Bug**

- When a large amount of metadata is updated concurrently, Kyligence Enterprise service may be automatically shut down
- When the original query is the same as the corrected query by Kyligence Enterprise, the query statement is not printed in log files
- The query is not cancelled when Spark gets data timeout
- When configuring row-level permissions for integer fields, the validity check was not performed, causing the query to fail
- The building job is stuck due to unreasonable configuration of Kerberos ticket life cycle and refresh interval
- When adjusting the Rowkey order of the fields, the column cannot be topped after being searched
- Add error message when the full load is triggered repeatedly
- Use Internet Explorer browser (version 11) to delete the model, the deletion fails
- Add error copy when the user group name is repeated
- Optimize the error message when the grammatical check fails in a computed column
- The expired test license still work in Kyligence Enterprise
- The query results of left join and right join that hit the same index are inconsistent
- Queries over 10s are recorded in Influxdb in the range of 5 to 10s.
- When generating recommendations by importing SQLs, if there are columns that were deleted after reloading the source table in the model hit by SQLs, generating recommendations may fail


#### Kyligence Enterprise 4.2.1 release note

**Enhancement**

- Fixed the responses of user_with_group API to ensure the integration with Kyligence Insight
- Avoid slow queries blocking simple queries.
- Support segment retention threshold in days as the unit of measurement.
- Increase the relevant log of HA function
- Support async query API.
- Optimize table reload scenario text
- Optimize callback interface return information, add job types, job IDs, and the response is integrated according to segment.
- Support using segment name as parameters in segment operation API.
- Optimize APIs of user deletion, user group creating and user group deletion to ensure that some users and user groups with special characters are added/deleted correctly.
- The display content of the node address in the unified interface
- Support to export TDS file from models.

**Bug**

- The element parameter in the exported TDS-api needs to have a default value and is an optional parameter
- Edit dimension name does not take effect
- The capacity billing function causes the flat meter to be calculated multiple times during building
- After synchronizing the annotations of the dimension column, the annotations are synchronized without saving the model.
- Failed to build the index because the snapshot could not be found
- In a high-availability environment, after the Query node is restarted, because the metadata is not properly synchronized, the new project added to the All node does not show - ry node.
- When the sum (constant) metric is defined in the model, the model saves an error.
- Click to generate license file on the login page, the page reports an error.
- Too many Spark tasks might be generated during the building job, which will influence the build performance
- In HA mode, when two KE nodes hold system and project management permissions respectively, projects cannot be deleted.
- The maximum query delay in the optimization proposal is not limited, resulting in an error on the interface after saving.
- When sql modeling, it can recommend the CC column with the same name, and report an error when passing.
- In the split_part query function, the query will report an error when the index exceeds the number of divided parts.
- Supports pagination when there are more recommendations
- Part of the log information level is wrong in check-env.out
- Trim function does not take effect
- When creating a model, the dimension cardinality is not displayed in the edit dimension interface.
- If the partition column is int data type and used as filter conditions, segment pruning doesn’t work properly
- Fix the error message when the dimension in the model cannot be deleted
- If the system administrator is changed to a normal user, the corresponding privileges are not updated.
- Null pointer exception occurs when setting measures
- When creating a model, the same data source table is referenced multiple times and defined as SCD2 type. After saving the model, it cannot be modified.
- Failed to delete user group due to cache
- The model status is inconsistent after using the metadata tool to upgrade 3x metadata
- When a calculable column is created and selected in the edit model measurement, the column type verification is not done when saving.


#### Kyligence Enterprise 4.2.0 release note

In this new release, Kyligence Enterprise supports **flexible index building**, which enables queries from business could get more faster response.

At the same time, a new **data volume statistics system** is added, which is convenient and flexible for users to monitor the usage in project and table level, and we support **slowly changing dimension type 2**, which helps to cover more comprehensive business scenario.

In addition, the existing **index management mechanism** has also been optimized and improved, allowing the AI augmented engine to provide better OLAP modeling, analysis and management assistance.

**Flexible Index Building**

A more flexible index building mode is provided, that is, selected indexes could be built into a specified segment. In most usage scenarios, while the building cost is reduced, the indexes could respond faster to the query needs of business. What's more, the maintenance and management costs of automated scheduling will be reduced as well.

**Data Volume Statistics System**

The data volume statistics system will provide data volume statistics on different levels, and historical data volume usage trends can also be viewed. While the platform manager could get and statistical data usage conveniently and swiftly, the system also satisfies the flexible management and control requirements of enterprise users on the use of resources.

**Slowly Changing Dimension Type 2**

The new version Kyligence Enterprise supports slowly changing dimensions (Type 2) based on self-maintaining history table, so the system could flexibly respond to the analysis requirements of slow changing dimensions in business. Based on reasonable models, Kyligence can support a wider range of business scenarios.

**Optimized Index Management Mechanism**

The existing index recommendation system is greatly optimized and a scorer is added to provide more efficient recommendations, so as to aid index management and enhance the availability and ease of use of index recommendations.



#### Kyligence Enterprise 4.1.9 release note

**Enhancement**

- Stability Optimization
  - Using the separate process to generate diagnosis package to avoid the influence on Kyligence instances
- Security Optimization
  - Support connect InfluxDB with SSL
- Tableau Compatibility Optimization
  - Support ascii, chr, space functions
- Optimize loading performance when there are plenty of source tables
- Add the project name to the common operations, such as query and build APIs within logs
- Improve the detection of hive warehouse access permissions at starting step
- Improve metadata restore tool and clarify the behavior expectations corresponding to each command
- Add the Spark encoding of the global dictionary in the configuration file

**Bugfix**

- Query node does not report metrics to InfluxDB
- When the snapshot file is too large, the metadata might be updated failed due to the long transaction
- When the table or column name used in the computed columns starts with a number, the implicit query might not hit the correct indexes
- If there is a repetitive expression of a computed column, the model can be saved normally but build failed
- After adding or modifying the model-level custom configurations, the deleted index will be automatically recovered
- When the Hive table stored as ORC and its partition column type is Date, incremental building failed
- When the query user does not have any column-level access permissions for a table, the select * statement will return all column information
- Some information was leaked by JS files
- When the project name contains the epoch, the forwarding request failed
- ROLE_ADMIN user group can be deleted through Rest API
- When the number of users is too large, the acceleration rule page fails to load the user list
- When building or merging Segments, the path of snapshot may not be updated correctly leading query failed
- After setting the filter conditions for the model with multiple joint tables, the creating flat table step failed

#### Kyligence Enterprise 4.1.8 release note

**Enhancement**

- Support separate configuration tool parameters
- Support a table in the same project can be used as a fact table and a dimension table
- Optimize the logic of automatic tuning of spark
- Optimize the prompt message after reloading the table
- Optimize the page display of the table in the interface
- Optimize the automatic adjustment of job tasks
- Diagnostic package copywriting optimization
- Provide callback API for build tasks
- Optimize the response time of table structure interface calls
- Configure the maximum number of characters for dimension measure names
- Add the function of stopping query
- Support CDH 6.2
- Add V4 version REST API of resume the build task
- Supports adding table-to-table association relationships to correct the correctness of derived queries

**Bugfix**

- Inconsistent results of index query and push-down query due to time zone processing issues
- When multiple sets of Hadoop clusters are configured, one set of clusters cannot be used and all sparks cannot be started.
- When building index in a cloud environment, the dimension table is too big, resulting in broadcast OOM
- Modifications to tables with the same name in different databases will have wrong effects
- Performing audit log recovery operations will cause NPE errors
- When the audit log is excessive, exporting the log will cause KE to stuck.
- When an error occurs when modifying a computable column, the error message is not clear
- Cannot create a project after the Query node has executed garbage cleanup
- Failed to reset ADMIN password
- When KE is started, the garbage cleaning and password reset cannot be successful
- User rewrite spark.executor.instance configuration does not take effect


#### Kyligence Enterprise 4.1.7 release note

**Enhancement**

- Support to turn on Sanity Check for building jobs to reduce possible incorrect building problems
  - Add parameter , which is enabled by default
  - When this parameter is turned on, it may cause a certain build performance degradation. In the laboratory environment, the build performance drops by about 2%
- Optimize system monitoring information and add Spark Task Queue Metrics into InfluxDB
  - The monitoring metrics of Query node are not supported
- Improve product usability
  - Optimize the permanent display of naming conventions and enhance the guidance when creating a name or password
  - Optimize the editing of computed columns, including only refreshing the relevant indexes after modifying the computed columns, etc.

**Bugfix**

- After enabling Kerberos in Cloudera CDH 6.3.1, the environment check may fail
- When there are multiple Cloudera CDH versions in the environment, it may fail to obtain the correct version, resulting in startup errors
- During cluster migrating, if the Snapshot file is not migrated correctly, the building jobs may not be completed
- In read-write separation deployment mode, the failure of obtaining *hadoop_conf*  may cause startup errors
- When the JVM memory is small, the memory may become full during "Save and Build Index", which may cause the service to be unavailable
- When the fact table is a view, checking the partition column format may cause the building jobs to take too long to execute
- When using row-level permissions, the query may incorrectly match the model
- If an error occurs when the dimension table is associated with a snapshot, it may cause the new building job to fail
- When the browser is Chrome 67 and below and IE browser, the pages of editing aggregation group, viewing aggregation group, and rewriting model settings may not work properly
- When multiple SQLs are imported for automatical modeling to establish an inner join model, if the partial matching configuration is enabled, SQL cannot be accelerated in batches
- After the Sum Expression function is turned on, the index cannot be hit when the query contains both Count Distinct and Sum Case measures
- When resetting the password of the system administrator, the history records in the  log file will be cleared, which may affect the ability to diagnose problems
- In the rewriting model configuration page and the project setting page, the optional time range of the automatic merge setting does not match, resulting in the retention setting cannot be submitted
- When importing SQL for modeling, if the SQL is in the editing state, the editing content will be lost when click Next
- When computed columns are added to the table index with sort, there may be duplicate computed columns which may cause the table index cannot be modified
- For an index that contains TOPN, if the order of the results in the query is from small to large, the index can also be incorrectly hit
- In the model editing page, the column description is only available in the fact table when editing the dimension



#### Kyligence Enterprise 4.1.6 release note

Enhancement

- Enhance the check of reloading source table
  - check whether there are ongoing jobs in the source table
  - check whether there will be a column with the same name after the source table is reloaded
  - only update the model related to the source table when reloading the table
- Improve the intersection function and support bitmap functions
- Enhance the implementation of obtaining Spark eventlog in the diagnostic package

Usability Improvements

- Optimize the data source module and improve the readability
- Optimize the model module and enrich the information when designing the models

Security Improvements

- cross-domain policy allows setting origin

BI Integration Improvements

- Users are now able to achieve MicroStrategy Single Sign-on with Kyligence Enterprise 4 and be able to reflect the row-level, column-level, and table-level access control of the Kyligence Enterprise in the MicroStrategy reports.

  > Note: this feature need to cooperate with Kyligence ODBC Driver 3.1.9.1003 and above.

Bug

- When the database name is not declared in the query statement, the query might fail due to failure to match the default database - If the imported file contains defined computed column names, it might fail to execute when using SQL to create models - The measures and aggregate indexes might not be updated after the data type of the column in the source table is modified
- When the metastore is MySQL, the query history result is empty when filtering by response time
- The query might fail if it contains computed column and SQL_WVARCHAR type when using pushdown engine.
- The computable column in the subquery might not be expanded into an expression when the query is answered by pushdown engine, which will cause the query failed
- After the model is broken due to the source table changes, it might not be repairable
- After 4.0 is upgraded to 4.1, the columnTypeName field in the query interface might be changed


#### Kyligence Enterprise 4.1.5 release note

**Enhancement**

- Support to edit computed columns

  > For the time being, we only support modifying the expressions of computable columns, while the modifying of the names of computable columns is not supported.

- Provide public API for model import and output

- Optimize the logic of obtaining garbage files in the cloud environment to ensure the normal operation of garbage cleaning

**Bugfix**

- Add a parameter  to control the size of the result set returned by the query
- In HA deployment mode, the number of active nodes displayed on different nodes may be different
- Fix the problem of inconsistent front-end and back-end verification in some forms to prevent possible security holes
- Upgrade Jackson version to 2.9.10.3 to fix possible security holes

- Calling index overview API may take a long time
- When the filter contains clauses of the form *NOT IN (constant, constant)*, the query may fail
- In the login state, if the wrong URL is entered, you can still enter the login page, and  log in normally by entering any password at this time
- When deleting the dimension table on the model editing page, it needs to be deleted twice to delete the dimension table correctly
- When only Query node is included in the cluster, the data source page error shows no permission
- In the return of the V2 version job recovery API, there may exist incorrect task status
- During the automatic tuning process, due to the incorrect calculation formula of Spark Executor Memory, the resulting value may be negative
- Regulate password reset behavior: when the system administrator resets the passwords of other users, there is no need to fill in the old password
- When the number of query result rows exceeds the set maximum value, the front end does not report an error
- When the name of the computed column differs only in case, the same-name-check is not performed correctly

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

if [[ "${WITH_SPARK}" = "1" ]]; then
    echo "BUILD STAGE 3 - Prepare spark..."
    sh build/script_newten/download-spark.sh      || { exit 1; }
else
    rm -rf build/spark
fi

if [[ "${WITH_THIRDPARTY}" = "1" ]]; then
    echo "BUILD STAGE 4 - Prepare influxdb..."
    sh build/script_newten/download-influxdb.sh      || { exit 1; }

    echo "BUILD STAGE 5 - Prepare grafana..."
    sh build/script_newten/download-grafana.sh      || { exit 1; }

    echo "BUILD STAGE 6 - Prepare postgresql..."
    sh build/script_newten/download-postgresql.sh      || { exit 1; }
else
    rm -rf build/influxdb
    rm -rf build/grafana
    rm -rf build/postgresql
fi

echo "BUILD STAGE 7 - Prepare and compress package..."
sh build/script_newten/prepare.sh ${MVN_PROFILE} || { exit 1; }
sh build/script_newten/compress.sh               || { exit 1; }

echo "BUILD STAGE 8 - Clean up..."
    
echo "BUILD FINISHED!"
