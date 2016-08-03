/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

KylinApp.constant('kapEnglishConfig', {
  user:{
    setting: 'Settings',//reset password
    changePassword: 'Change password',
    oldPassword: 'Old password',
    newPassword: 'New password',
    confirmNewPassword: 'Confirm new password',
    tip_error_not_same: 'Password and confirm password are not the same.',
    updatePassword: 'Update Password',
    tip_username_invalid: 'Username invalid.',
    tip_password_invalid: 'Password invalid.',
    success_resetSuccess: 'Reset password successfully.',
    failed_reset: 'Failed to reset password.',
    users: 'Users',
    submit: 'Submit',
    close: 'Close',
    addUser: 'Add User',
    password: 'Pssword',
    role: 'Role',
    analyst: 'Analyst',
    modeler: 'Modeler',
    admin: 'Admin',
    editUser: 'Edit User',
    user: 'User',
    usertheaditems: [
      {attr: 'username', name: 'Username'},
      {attr: 'admin', name: 'Admin'},
      {attr: 'modeler', name: 'Modeler'},
      {attr: 'analyst', name: 'Analyst'},
      {attr: 'disabled', name: 'Status'}
    ],
    editRole: 'Edit Role',
    resetPassword: 'Reset Password',
    drop: 'Drop',
    enable: 'Enable',
    disable: 'Disable',
    alert_sure_to_drop: 'Are you sure to drop the user ?',
    success_drop: 'Drop user successfully',
    success_disable: 'Disable user successfully',
    success_enable: 'Enable user successfully',
    tip_failed_list_users: 'Failed to list users.',
    success_add_user: 'Add user successfully',
    success_update_user: 'Update user successfully'
  },
  login:{
    username: 'Username',
    password: 'Password',
    loginIssue: 'Login Issue?',
    loginIn: 'Login In',
    loginInNow: 'Login In...',
    error: 'Unable to login, please check your username/password.'
  },
  header:{
  chooseProject: 'Choose Project',
  login: 'Login',
  logout: 'Logout',
  help: 'Help',
  welcome: 'Welcome',
  documents: [
    {link:'http://kyligence.io',displayName:'About us'},
    {link:'https://www.gitbook.com/book/kyligence-git/kap-user-manual/details',displayName:'User Manual'},
    {link:' https://www.gitbook.com/book/kyligence-git/kap-admin-manual/details',displayName:'Admin Manual',hasAdmin:true}
  ],
  selectLanguage: 'select language'
  },
  /*
   module of  project
   */
  project:{
    /*
    part of manage project
     */
    info_no_result: 'No Result.',
    info_no_project: 'No Project.',
    info_load_project: 'Loading Projects...',
    theaditems: [
      {attr: 'name', name: 'Name'},
      {attr: 'owner', name: 'Owner'},
      {attr: 'description', name: 'Description'},
      {attr: 'create_time', name: 'Create Time'}
    ],
    action: 'Action',
    manageProject: 'Manage project',
    ac_project_edit: 'Edit',
    ac_project_delete: 'Delete',
    cubes: 'Cubes',   //project-cubes begin
    cubeName: 'Cube Name',
    cubeActions: 'Actions',
    ac_cube_detail: 'detail',
    access: 'Access',  //project-access begin
    ac_access_grant: 'Grant',
    ac_access_cancel: 'Cancel',
    tip_access: '<div style="text-align: left"><label>What does access mean to cube?</label><ul><li>CUBE QUERY: Access to query cube</li><li>CUBE OPERATION: Access to rebuild, resume and cancel jobs. Also include access of CUBE QUERY.</li><li>CUBE MANAGEMENT: Access to edit/delete cube. Also include access of CUBE OPERATION.</li><li>CUBE ADMIN: Full access to cube and jobs, including access management.</li></ul></div>',
    accessName: 'Name',
    plac_accessName: 'User NT Account...',
    accessType: 'Type',
    accessTypeUser: 'User',
    accessTypeRole: 'Role',
    accessAccess: 'Access',
    accessUpdate: 'Update',
    accessPermission: 'Permission',
    chooses_accessUpdate_deaf:'-- select access -- ',
    chooses_accessUpdate: {
                    READ: {name: 'CUBE QUERY', value: 'READ', mask: 1},
                    MANAGEMENT: {name: 'CUBE EDIT', value: 'MANAGEMENT', mask: 32},
                    OPERATION: {name: 'CUBE OPERATION', value: 'OPERATION', mask: 64},
                    ADMINISTRATION: {name: 'CUBE ADMIN', value: 'ADMINISTRATION', mask: 16}
                 },
    accessRevoke: 'Revoke',
    externalFilters: 'External Filters', //project-External Filters begin
    ac_externalFilters_filter: 'Filter',
    efNewFilter: 'New Filter',
    efFilterName: 'Filter Name',
    plac_efFilterName: 'You can use letters, numbers, and underscore characters ',
    checkInfo_require_efFilterName: 'The filter name is required',
    checkInfo_invalid_efFilterName: 'The filter name is invalid',
    efTableType: 'Table Name',
    efResourcePath: 'Resource Path',
    plac_efResourcePath: 'You can use letters, numbers, and underscore characters "_"',
    checkInfo_require_efResourcePath: 'The resource path is required',
    checkInfo_invalid_efResourcePath: 'The resource path is invalid',
    efDescription: 'Description',
    plac_efDescription: 'Filter Description..',
    ac_efClose: 'Close',
    ac_efSubmit: 'Submit',
    /*
    part of add project
     */
    addProject: 'Project',
    addAdd: 'Add Project',
    addTitle: 'New Project',
    addProjectName: 'Project Name',
    plac_addProjectName: 'You can use letters, numbers, and underscore characters "_"',
    checkInfo_require_addProjectName: 'The project name is required',
    checkInfo_invalid_addProjectName: 'The project name is invalid',
    addProjectDescription: 'Description',
    plac_addProjectDescription: 'Project Description..',
    addClose: 'Close',
    addSubmit: 'Submit',
    total:'Total' //sum of project
  },
  name:'kylin',
  /*
   module of model
   */
  model:{
    modelClone: 'MODEL CLONE',//cube clone
    targetProject: 'Target Project is',
    selectProject: 'select a project',
    newModelName: 'New Model Name:',
    submit: 'Submit',
    close: 'Close',
    /*
     management of model
     */
    Model: 'Model',
    Measure: 'Measure',
    paramValue: 'Param Value',
    model: 'Model',
    schema:['Model Info','Data Mode','Dimensions','Measures','Settings'],
    models: 'Models',
    theaditems : [
      {attr: 'name', name: 'Name'},
      {attr: 'project', name: 'Project'},
      {attr: 'fact_table', name: 'Fact Table'},
      {attr: 'last_modified', name: 'Last Modified Time'}
    ],
    actions: 'Actions',
    wizardSteps: [
      {title: 'Model Info', src: 'partials/modelDesigner/model_info.html', isComplete: false, form: 'model_info_form'},
      {title: 'Data Model', src: 'partials/modelDesigner/data_model.html', isComplete: false, form: 'data_model_form'},
      {
        title: 'Dimensions',
        src: 'partials/modelDesigner/model_dimensions.html',
        isComplete: false,
        form: 'model_dimensions_form'
      },
      {
        title: 'Measures',
        src: 'partials/modelDesigner/model_measures.html',
        isComplete: false,
        form: 'model_measure_form'
      },
      {
        title: 'Settings',
        src: 'partials/modelDesigner/conditions_settings.html',
        isComplete: false,
        form: 'model_setting_form'
      }
    ],
    info_no_model: 'No Model',
    info_create_model: 'Click here to create your model',
    next: 'Next',
    prev: 'Prev',
    save: 'Save',
    grid: 'grid',    //grid of model begin
    gridModelDesign: 'Model Designer',
    gridModelInfo: 'Model Info',
    gridMIModelName: 'Model Name',
    tip_body_model_name: '<li>Model name is unique name of entire system.</li><li>Can not edit model name after created.</li>',
    check_info_required_model_name: 'Model name is required.',
    check_info_invalid_model_name: 'Model name is invalid.',
    gridMIDescription: 'Description',
    check_info_warning_MI: '<div class="box box-danger"><div class="box-header with-border"> <i class="fa fa-warning"></i><h3 class="box-title">Warning!</h3></div> <div class="box-body callout callout-warning"><ol><li>Project can\'t be empty</li><li>Model Name can\'t be empty</li></ol></div></div>',
    gridDataModel: 'Data Model',
    gridDMFactTable: 'Fact Table',
    gridDMLookupTables: 'Lookup Tables',
    gridDMNoLookupTables: 'No Lookup Tables',
    gridDMID: 'ID',
    gridDMTableName: 'Table Name',
    gridDMJoinType: 'Join Type',
    gridDMJoinCondition: 'Join condition',
    gridDimensions: 'Dimensions',
    gridDSID: 'ID',
    gridDSTableName: 'Table Name',
    gridDSColumns: 'Columns',
    gridMeasure: 'Measure',
    gridMSID: 'ID',
    gridMSColumn: 'column',
    gridMSSelectMeasure: 'Select your measures',
    gridSettings: 'Settings',
    gridSTPartition: 'Partition',
    gridSTPartitionType: 'Partition Type',
    gridSTPartitionDate: 'Partition Date Column',
    tip_gridSTPartitionDate: '<li>Partition date column not required,leave as default if cube always need full build</Li><li>Column should contain date value (type can be Date, Timestamp, String, VARCHAR, etc.)</li>',
    gridSTPartitionDateSelect: '--Select Partition Column--',
    gridSTPartitionDateFormat: 'Date Format',
    gridSTPartitionDateFormatSelect: '--Select Date Format--',
    gridSTSetSeparate: 'Has a separate "time of the day" column ?',
    tip_tile_gridHasSepTimeColumn: 'Separate Time Column',
    tip_gridSTSetSeparate: 'For cases where fact table saves date and hour at two columns (KYLIN-1427)',
    gridHasParTimeColumn: 'Partition Time Column',
    tip_gridParTimeColumn: 'Column should contain time value (type can be Timestamp, String, VARCHAR, etc.)',
    gridHasSepTimeColumnSelect: '--Select Partition Column--',
    gridHasSepTimeColumnTF: 'Time Format',
    gridHasSepTimeColumnTFSelect: '--Select Time Format--',
    gridSTFilter: 'Filter',
    gridSTWhere: 'Where',
    edit: 'Edit',  //edit of model begin
    editDMSelectFact: '-- Select Fact Table --',
    editDMAddLookupTable: 'Add Lookup Table',
    editDMSelectLookup: '-- Select Lookup Table --',
    editDMActions: 'Actions',
    editDMLookupAdd: 'Add',
    editDMLookupAdd: 'Edit',//lookup table modal
    editDMLookupTableName: 'Lookup Table Name',
    editDMLookupNewJoin: ' New Join Condition',
    tip_join_delete: 'delete',
    tip_header_lookup_modal: ' <h4 class="box-title">Tips</h4>',
    tip_body_lookup_modal: '<ol class="text-info"><li>Pick up lookup table at first</li><li>Specify join relationship between fact table and chosen lookup table.</li><li>Join Type have to be same as will be used in query</li></ol>',
    ac_lookup_ok: 'OK',
    ac_lookup_cancel: 'Cancel',
    clone: 'Clone',
    drop: 'Drop',
    visualization: 'Visualization'
  },
  cube:{
    cubeClone: 'CUBE CLONE',//cube clone
    targetProject: 'Target Project is',
    selectProject: 'select a project',
    newCubeName: 'New Cube Name:',
    submit: 'Submit',
    close: 'Close',
    cubeName: 'Cube Name',
    tip_cube_required: 'Cube name is required.',
    tip_cube_name_invalid: 'Cube name is invalid.',
    cubeCubeDesigner: 'Cube Designer',
    cubeSave: 'Save',
    cubeNext: 'Next',
    cubePrev: 'Prev',
    cubesIn: '-- All --',
    ac_cube: 'Cube',
    aggregationGroups: 'Aggregation Groups',
    tip_visit: 'visit',
    tip_about_aggregation_group:'for more about aggregation group.',
    theaditems: [
      {attr: 'name', name: 'Name'},
      {attr: 'detail.model_name', name: 'Model'},
      {attr: 'status', name: 'Status'},
      {attr: 'size_kb', name: 'Cube Size'},
      {attr: 'input_records_count', name: 'Source Records'},
      {attr: 'last_build_time', name: 'Last Build Time'},
      {attr: 'owner', name: 'Owner'},
      {attr: 'create_time_utc', name: 'Create Time'}
    ],
    actions: 'Actions',
    schema: ['Cube Info','Dimensions','Measures','Refresh Setting','Advanced Setting','Configuration Overwrites','Overview'],
    admins: 'Admins',
    storage: 'Storage',
    streaming: 'Streaming',
    cubeActions:['Drop','Edit','Build','Refresh','Merge','Disable','Enable','Purge','Clone'],
    tip_drop_cube: 'Drop the cube, related jobs and data permanently.',
    edit: 'Edit',
    SegmentId: 'Segment ID',
    cubeItems: ['Grid','SQL','JSON(Cube)','Access','Notification','Storage'],
    cubeNoNotification: 'Notification List(Common Separated)',
    ac_cubeNotification_save: 'Save',
    cubeHBHTable: 'HTable:',
    cubeHBRegionCount: 'Region Count:',
    cubeHBSize: 'Size:',
    cubeHBStartTime: 'Start Time:',
    cubeHBEndTime: 'End Time:',
    cubeHBTotalSize:  'Total Size:',
    cubeHBTotalNumber: 'Total Number:',
    cubeHBNoHBaseInfo: 'No HBase Info.',
    cubeCIModelName: 'Model Name',
    cubeCIAddChooseModel: '-- Choose Model --',
    tip_title_cube_name: 'cube_name',
    tip_body_cube_name: '<li>Cube name is unique name of entire system.</li><li>Can not edit cube name after created.</li>',
    cubeCINotifiEmail: 'Notification Email List',
    cubeCINotifiEvents: 'Notification Events',
    tip_body_cubeCINotifiEvents: 'ERROR status will trigger a notification anyway.',
    cubeCIDescription: 'Description',
    cubeDSAddDS: 'Add Dimension',
    cubeDSNormal: 'Normal',
    cubeDSDerived: 'Derived',
    cubeDSSwitchDer: 'derived',
    cubeDSColumn: 'Column',
    cubeDSSwitchNormal: 'normal',
    cubeDSAutoGenerator: 'Auto Generator',
    cubeDSID: 'ID',
    cubeDSName: 'Name',
    tip_cubeDSName: 'Name..',
    tip_cubeDSDiName: 'dimension name..',
    cubeDSTableName: 'Table Name',
    tip_select_table: 'Select a table',
    tip_select_lookup: 'Select a lookup table',
    cubeDSType: 'Type',
    cubeDSActions: 'Actions',
    info_no_following_columns: 'There are following columns defined in multiple dimensions. <small>This is not recommended!</small>',
    cubeDSEditDSEdit: 'Edit Dimension',
    cubeDSEditDSAdd: 'Add Dimension',//add dimension begin
    tip_no_dimension_name: 'The Dimension Name is required.',
    tip_derived_dimension: 'Derived dimension only comes from lookup table.',
    cubeDSColumnName: 'Column Name',
    cubeDSSelectColumn: '-- Select Column --',
    ac_delete_cubeDSColumn: 'Delete',
    cubeDSNewHierarchy: 'New Hierarchy',
    tip_cubeDSNewHierarchy: 'Drag level into the order you prefer.',
    cubeDSNewDerived: 'New Derived',
    tip_title_cubeDS: 'Tips',
    tip_body_cubeDS: ' <li>Type in any input box for auto suggestion</li><li>Pick up Fact Table from Star Schema Tables first</li><li>Data Type should match with Hive Table\'s Data Type</li><li>Join Type have to be same as will be used in query</li><li>Using Derived for One-One relationship between columns, like ID and Name</li>',
    ac_ok_cube: 'OK',
    ac_cancel_cube: 'Cancel',
    cubeDSAutoGenerate: 'Auto Generate Dimensions <small>This is a helper for you to batch generate dimensions.</small> ',//Auto Generate Dimensions begin
    cubeDSColumns: 'Columns',
    cubeDSFactTable: '[Fact Table]',
    cubeDSLookupTable: '[Lookup Table]',
    tip_title_DSAutoGenerate: 'Auto Generate Rules',
    tip_body_DSAutoGenerate: '<li>Column is disabled once there is already a dimension referred this column.</li><li>In fact table: one normal dimension will be auto generated per column.</li><li>In lookup table: one derived dimension will be auto generated for all the selected columns.</li>',
    cubeMSName: 'Name',//measures begin
    cubeMSExpression: 'Expression',
    cubeMSParameters: 'Parameters',
    cubeMSParamType: 'Param Type',
    cubeMSParamValue: 'Param Value',
    tip_cubeMSParamValue: '<li>Only accept single column in param value with "Column" type</li><li>Distinct Count is approximate, please indicate Error Rate, higher accuracy degree accompanied with larger storage size and longer build time</li>',
    cubeMSParamValueSelect: '-- Select a Column --',
    cubeMSConstant: 'constant',
    cubeMSReturnType: 'Return Type',
    cubeMSActions: 'Actions',
    cubeMSEditMS: 'Edit Measure',
    cubeMSMeasureName: 'measure name..',
    tip_cubeMSExpression: 'All cubes have to contain one measure for Count(1), suggest use "_Count_" as name (Has been generated automatically)',
    cubeMSType: 'Type',
    cubeMSValue: 'Value',
    cubeMSGB: 'Group By Column',
    cubeMSOK: 'OK',
    cubeMSCancel: 'Cancel',
    cubeMSParameterAdd: 'Add Parameter',
    cubeMSParameterSelect: 'Select Group By Column',
    cubeRSAutoMerge: 'Auto Merge Thresholds',//refresh_settings begin
    tip_cubeRSAutoMerge: 'The thresholds will be checked ascendingly to see if any consectuive segments\' time range has exceeded it. For example the [7 days, 30 days] will result in daily incremental segments being merged every 7 days, and the 7-days segments will get merged every 30 days.',
    cubeRSNewThresholds: 'New Thresholds',
    cubeRSRetentionThreshold: 'Retention Threshold',
    tip_cubeRSRetentionThreshold: 'By default it\'s \'0\',which will keep all historic cube segments ,or will keep latest [Retention Threshold] days cube segments.',
    cubeRSPartitionStart: 'Partition Start Date',
    tip_cubePartitionDate: 'Please input start date when partition date column is defined in model.',
    cubeASID: 'ID',//Advanced Setting begin
    cubeASAggregationGroups: 'Aggregation Groups',
    cubeASIncludes: 'Includes',
    cubeASMandatoryDimensions: 'Mandatory Dimensions',
    plac_cubeAS: 'Select Column...',
    cubeASHierarchyDimensions: 'Hierarchy Dimensions',
    cubeASNewHierarchy: 'New Hierarchy',
    cubeASJointDimensions: 'Joint Dimensions',
    cubeASNewJoint: 'New Joint',
    cubeASNewAggregationGroup: 'New Aggregation Group',
    cubeASRowkeys: 'Rowkeys',
    tip_title_cubeASRowkeys: 'Rowkey',
    tip_body_cubeASRowkeys: '<h4>Shard By this column?</h4><p>If specified as "true", cube data will be sharded according to its value.</p><h4>RowKey Encoding</h4><ol><li>"dict" encoding will try to build a dictionary for the dimension</li><li>"fixed_length" encoding will encode the dimension vlaues by fixed length bytes with padding</li><li>"int" encoding uses variable integer to encode integers </li></ol>',
    cubeASID: 'ID',
    cubeASColumn: 'Column',
    cubeASEncoding: 'Encoding',
    cubeASLength: 'Length',
    cubeASShardBy: 'Shard By',
    tip_rowkey_column_name: 'rowkey column name..',
    tip_rowkey_column_length: 'rowkey column length..',
    tip_false_by_default: 'false by default',
    tip_Column_Name: 'Column Name..',
    cubeASNewRowkeyColumn: 'New Rowkey Column',
    plac_cubeCOKey: 'Key',//Configuration Overwrites begin
    tip_cubeCOKey: 'Property value is required.',
    plac_cubeCOValue: 'Value',
    tip_cubeCOValue: 'Property value is required.',
    cubeCOProperty: 'Property',
    cubeCOTips: 'Tips',
    tip_cubeCOTips: 'Cube level properties will overwrite configuration in kylin.prperties',
    cubeOVModelName: 'Model Name',//Overview begin
    cubeOVCubeName: 'Cube Name',
    cubeOVFactTable: 'Fact Table',
    cubeOVLookupTable: 'Lookup Table',
    cubeOVDimensions: 'Dimensions',
    cubeOVMeasures: 'Measures',
    cubeOVDescription: 'Description',
    tip_loading_cubes: 'Loading Cubes...',
    kaTopic: 'Topic', //kafka begin
    tip_Kafka_topic: 'Kafka topic is required.',
    tip_kafka_topic_invalid: 'Kafka topic is invalid.',
    kaCluster: 'Cluster',
    tip_input_kafkaConfig_topic: 'Input kafkaConfig topic',
    kaHost: 'Host',
    kaPort: 'Port',
    kaActions: 'Actions',
    tip_input_broker: 'Input broker ID',
    tip_input_broker_host: 'Input broker host',
    tip_input_broker_port: 'Input broker port',
    kaBroker: 'Broker',
    kaSave: 'Save',
    kaCancel: 'Cancel',
    kaKafkaSetting: 'Kafka Setting',
    kaAdvancedSetting: 'Advanced Setting',
    kaTimeout: 'Timeout',
    tip_input_kafkaConfig_timeout: 'Input kafkaConfig timeout',
    tip_timeout_invalid: 'Kafka timeout is invalid.',
    tip_timeout_required: 'Kafka timeout is required.',
    kaBufferSize: 'Buffer Size',
    tip_kafkaConfig_bufferSize: 'Input kafkaConfig bufferSize',
    tip_bufferSize_invalid: 'Kafka bufferSize is invalid.',
    tip_bufferSize_required: 'Kafka bufferSize is required.',
    kaMargin: 'Margin',
    tip_input_kafkaConfig_margin: 'Input kafkaConfig margin',
    tip_kafka_margin_invalid: 'Kafka margin is invalid.',
    tip_kafka_margin_required: 'Kafka margin is required.',
    kaParserSetting: 'Parser Setting',
    kaParserName: 'Parser Name',
    tip_input_kafkaConfig_parserName: 'Input kafkaConfig parserName',
    tip_kafka_parser_name_invalid: 'Kafka parser name is invalid.',
    tip_kafka_parser_name_required: 'Kafka parser name is required.',
    kaParserTimestampColumn: 'Parser Timestamp Column',
    kaSelectColumn: 'select a column',
    kaParserProperties: 'Parser Properties',
    tip_parser_properties_invalid: 'Parser properties is invalid.',
    tip_parser_properties_required: 'Parser properties is required.'
  },
  monitor:{
    monitor: 'Monitor',
    jobs: 'Jobs',
    slowQueries: 'Slow Queries',
    jobsCubeName: 'Cube Name:',
    plac_jobsCubeName: 'Filter ...',
    jobsJobsIn: 'Jobs in',
    jobConfig:{},//引用jobConfig.js
    tip_no_job: 'No Job.',
    tip_loading_jobs: 'Loading Jobs ...',
    jobResume: 'Resume',
    jobDiscard: 'Discard',
    jobDiagnosis: 'Diagnosis',
    tip_jobDiagnosis: 'Download Diagnosis Info For This Job',
    jobsDetailInfo: 'Detail Information',//job detail begin
    tip_jobsDetailInfo: 'Collapse',
    jobsJobName: 'Job Name',
    jobsJobID: 'ID',
    jobsStatus: 'Status',
    jobsDuration: 'Duration',
    jobsMapReduceWaiting: 'MapReduce Waiting',
    jobsStart: 'Start',
    jobsStepName: 'Step Name:',
    jobsDataSize: 'Data Size:',
    tip_Parameters: 'Parameters',
    tip_Log: 'Log',
    tip_MRJob: 'MRJob',
    jobsEnd: 'End',
    tip_no_job_selected: 'No Job Selected.',
    jobsModalParameters: 'Parameters',
    jobsModalOutput: 'Output',
    jobsModalLoading: 'loading ...',
    jobsModalCmdOutput: 'cmd_output',
    jobsModalClose: 'Close',
    jobsModalSubmit: 'Submit',
    jobsCubeMergeConfirm: 'CUBE MERGE CONFIRM',//jobs merge modal begin
    jobsNOSegment: 'NO SEGMENT TO MERGE.',
    jobsPartitionDate: 'PARTITION DATE COLUMN',
    jobsMergeStart: 'MERGE START SEGMENT',
    jobsMergeEnd: 'MERGE END SEGMENT',
    jobsSegmentDetail: 'START SEGMENT DETAIL',
    jobsStartDate: 'Start Date (Include)',
    jobsEndDate: 'End Date (Exclude)',
    jobsLastbuildTime: 'Last build Time',
    jobsLastbuildID: 'Last build ID',
    jobsEndSegment: 'END SEGMENT DETAIL',
    tip_cube_segment: 'There exists a build request of this cube, the job may be running or error. If you need a new build, please wait for its complete or discard it.',
    tip_no_partition: 'No partition date column defined.',
    jobsCubeRefreshConfirm: 'CUBE REFRESH CONFIRM',//jobs refresh modal begin
    jobsNoSegmentRefresh: 'No Segment to refresh.',
    jobsPartitionDateColumn: 'PARTITION DATE COLUMN',
    jobsRefreshSegment: 'REFRESH SEGMENT',
    jobsSegmentDetail: 'EGMENT DETAIL',
    jobsCubeBuildConfirm: 'CUBE BUILD CONFIRM',//jobs submit modal begin
    tip_no_project_selected: 'No project selected',//job slow query begin
    tip_no_slow_query: 'No Slow Query.',
    tip_loading_queries: 'Loading Queries ...',
  },
  insight:{
    insight: 'Insight',
    tables: 'Tables',//query.html begin
    tip_no_table: 'No Table.',
    tip_loading_Tables: 'Loading Tables...',
    newQuery: 'New Query',
    savedQueries: 'Saved Queries',
    queryHistory: 'Query History',
    tip_list_tables: 'Tips: Ctrl+Shift+Space or Alt+Space(Windows), Command+Option+Space(Mac) to list tables/columns in query box.',
    project: 'Project:',
    limit: 'LIMIT',
    ac_submit: 'Submit',
    sqlName: 'SQL Name:',
    description: 'Description',
    ac_resubmit: 'Resubmit',
    ac_remove: 'Resubmit',
    tip_no_query_history: 'No Query History.',
    queriedAt: 'Queried At: ',
    inProject: 'in Project:',
    status: ' Status:',
    tip_no_query_result: 'No Query Result.',
    tip_no_saved_query: 'No Saved Query.',
    startTime: 'Start Time:',//query detail begin
    duration: 'Duration: ',
    ac_return: 'Rerun',
    ac_save: 'Save',
    queryString: 'Query String',
    queryStatus: ['Failed','Success','Executing...'],
    cubes: 'Cubes',
    results: 'Results',
    tip_curQuery_result_partial: '!Note: Current results are partial, please click \'Show All\' button to get all results.',
    showAll: 'Show All',
    visualization: 'Visualization',
    grid: 'Grid',
    ac_export: 'Export',
    tip_loading: 'Loading...',
    tip_more: 'More',
    graphType: 'Graph Type',
    dimensions: 'Dimensions',
    chooseDimension: '-- choose dimension --',
    metrics: 'Metrics',
    plac_select_metrics: 'Select Metrics..',
    chooseMetrics: '-- choose metrics --',
    tip_no_graph_generated: 'No Graph Generated.',
    saveQuery: 'Save Query', //Save Query Modal begin
    project: 'Project',
    name: 'Name',
    plac_name: 'Name..',
    plac_description: 'Description..',
    ac_close: 'Close'
  },
  data_source:{
    data_source: 'Data Source',
    tables: 'tables',//--table tree begin
    tip_load_hive_table: 'Load Hive Table',
    tip_load_table_tree: 'Load Hive Table From Tree',
    tip_unLoad_hive_table: 'UnLoad Hive Table',
    tip_add_streaming_table: 'Add Streaming Table',
    titleAddHiveTable: 'Load Hive Table Metadata',//modal addHiveTable begin
    project: 'Project:',
    tableNames: 'Table Names:(Seperate with comma)',
    plac_table_with_comma: 'table1,table2  By default,system will choose \'Default\' as database,you can specify database like this \'database.table\'',
    sync: 'Sync',
    cancel: 'Cancel',
    titleAddHiveTableFromTree: 'Load Hive Table Metadata From Tree',//modal addHiveTableFromTree begin
    plac_filter: 'Filter ...',
    tip_loading_databases: 'Loading Databases...',
    tip_loading_tables: 'Loading Tables...',
    ac_show_more: 'Show More',
    ac_show_all: 'Show All',
    titleRemoveHiveTable: 'UnLoad Hive Table Metadata',//modal removeHiveTable begin
    titleAddStreamingSource: 'Streaming Table And Cluster Info',//modal addStreamingSource begin
    prev: 'Prev',
    next: 'Next',
    submit: 'Submit',
    //part of modal addStreamingSource-- loadStreamingTable  --step 1
    info_loadStreamingTable: 'Need to input streaming source record here, will detect the source schema and create a table schema for streaming.',
    tip_streaming_sourceSchema: 'input Streaming source record to generate schema.',
    tip_table_schemaChecked: 'Source json invalid, Please correct your schema and generate again.',
    tip_table_schemaChecked_detail: '<li>Choose \'timestamp\' type column for streaming table.</li><li>By default, system will choose \'Default\' as database, you can specify database like this \'database.table\'</li><li>derived time dimensions are calculated from timestamp field to help analysis against different time granularities.</li>',
    tableName: 'Table Name',
    tip_table_name: 'Table name is required.',
    column: 'Column',
    columnType: 'Column Type',
    comment: 'Comment',
    plac_select_column_type: 'select a column type',
    timestamp: 'timestamp',
    derivedTimeDimension: 'derived time dimension',
    tip_choose_timestamp: 'You should choose at least one \'timestamp\' type column generated from source schema.',
    tableSchema: 'Table Schema:',//--table detail begin
    columns: 'Columns',
    extendInforFDFmation: 'Extend Information',
    streamingCluster: 'Streaming Cluster',
    name: 'NAME',
    hiveDatabase: 'Hive DATABASE',
    snapshotTime: 'SNAPSHOT TIME',
    location: 'lOCATION',
    inputFormat: 'INPUT FORMAT',
    outputFormat: 'OUTPUT FORMAT',
    owner: 'OWNER',
    totalFileNumber: 'TOTAL FILE NUMBER',
    totalFileSize: 'TOTAL FILE SIZE',
    partitioned: 'PARTITIONED',
    partitionColumns: 'PARTITION COLUMNS',
    plac_column: 'Filter ...',
    theaditems: [
      {attr: 'id', name: 'ID'},
      {attr: 'name', name: 'Name'},
      {attr: 'datatype', name: 'Data Type'},
      {attr: 'cardinality', name: 'Cardinality'}
    ],
    tip_no_table_selected: 'No Table Selected.',
    noTables: 'No tables',
    tip_load_hive_table: 'Click here to load your hive table',
    tip_no_project_selected: 'No project selected',
    tip_select_project: 'Please select your project first',
    tip_select_table: 'Select your table'
  },
  system:{
    system: 'System',
    serverConfig: 'Server Config ',
    serverEnvironment: 'Server Environment ',
    actions: 'Actions',
    reloadMetadata: 'Reload Metadata',
    tip_reloadMetadata: 'Reload all metadata and clean cache',
    calculateCardinality: 'Calculate Cardinality',
    tip_calculateCardinality: 'Calculate cardinality of tables',
    cleanUpStorage: 'Clean Up Storage',
    tip_cleanUpStorage: 'Clean unused HDFS and HBASE space',
    disableCache: 'Disable Cache',
    enableCache: 'Enable Cache',
    tip_enableCache: 'Enable Query Cache',
    setConfig: 'Set Config',
    tip_setConfig: 'Update Server Config',
    diagnosis: 'Diagnosis',
    tip_diagnosis: 'Download Diagnosis Info For Current Project',
    links: 'Links',
    loadHiveTable: 'Load Hive Table Metadata',//modal calCardinality begin
    tableName: 'Table Name:',
    plac_tableName: 'table1,table2',
    delimiter: 'Delimiter:',
    valueDeliRadios: 'default -- System will try to get the format from hive metadata, if you are not sure, please choose the "default".',
    formatRadios: 'default -- System will try to get the format from hive metadata, if you are not sure, please choose the "default".',
    text: 'text',
    sequence: 'sequence',
    sync: 'Sync',
    cancel: 'Cancel',
    updateConfig: 'Update Config',//modal updateConfig begin
    key: 'Key',
    tip_key: 'Config Key..',
    tip_required_key: 'Key is required.',
    value: 'Value',
    tip_value: 'Config Value..',
    tip_required_value: 'Value is required.',
    update: 'Update'
  },
  alert:{
    error_info: 'Failed to take action.',
    success_grant: 'Access granted', //alert of AccessCtrl
    userNotFound: 'User not found!!',
    tip_sure_to_revoke: 'Are you sure to revoke the access?',
    success_access_been_revoked: 'The access has been revoked.',
    tip_to_reload_metadata: 'Are you sure to reload metadata and clean cache?',//alert of AdminCtrl
    success_server_environment: 'Server environment get successfully',
    success_server_config: 'Server config get successfully',
    success_cache_reload: 'Cache reload successfully',
    tip_clean_HDFS_HBase: 'Are you sure to clean up unused HDFS and HBase space?',
    success_storage_cleaned: 'Storage cleaned successfully!',
    success_cache_disabled: 'Cache disabled successfully!',
    tip_enable_query_cache: 'Are you sure to enable query cache?',
    success_cache_enabled: 'Cache enabled successfully',
    success_cardinality_job: 'Cardinality job was calculated successfully. . Click Refresh button ...',
    success_config_updated: 'Config updated successfully!',
    tip_no_project_selected: 'No project selected.',
    tip_to_disable_cache: 'Are you sure to disable query cache?',
    error_failed_to_load_query: 'Failed to load query.',//alert of BadQueryCtrl;
    success_notify_list_updated: 'Notify List updated successfully!',//alert of CubeCtrl
    tip_select_project: 'Please select your project first.',//alert of CubeEditCtrl
    tip_invalid_cube: 'Invalid cube json format..',
    tip_to_save_cube: 'Are you sure to save the cube ?',
    success_updated_cube: 'Updated the cube successfully',
    success_created_cube: 'Created the cube successfully.',
    tip_column_required: '[TOP_N] Group by Column is required',//alert of CubeMeasuresCtrl
    tip_remove_lookup_table: 'Once it\'s removed, all relative dimensions will be removed. Are you sure to remove the lookup table?',//alert of CubeModelCtrl
    tip_no_cube_detail: 'No cube detail info loaded.',//alert of cubesCtrl
    tip_no_cube_detail_loaded: 'No cube detail info loaded.',
    tip_sure_to_enable_cube: 'Are you sure to enable the cube? Please note: if cube schema is changed in the disabled period, all segments of the cube will be discarded due to data and schema mismatch.',
    success_Enable_submitted: 'Enable job was submitted successfully',
    tip_to_purge_cube: 'Are you sure to purge the cube?',
    success_purge_job: 'Purge job was submitted successfully',
    tip_disable_cube: 'Are you sure to disable the cube? ',
    success_disable_job_submit: 'Disable job was submitted successfully',
    tip_metadata_cleaned_up: 'Once it\'s dropped, your cube\’s metadata and data will be cleaned up and can\’t be restored back. ',
    tip_cube_drop: 'Cube drop is done successfully',
    tip_to_start_build: 'Are you sure to start the build ?',
    success_rebuild_job: 'Rebuild job was submitted successfully',
    tip_select_target_project: 'Please select target project.',
    tip_to_clone_cube: 'Are you sure to clone the cube? ',
    tip_for_clone_project: 'Cross project clone is not allowed now,  cube will be cloned into current project.',
    success_clone_cube: 'Clone cube successfully',
    success_rebuild_job: 'Rebuild job was submitted successfully',
    tip_rebuild_part_one: 'Empty cube segment found',
    tip_rebuild_part_two: ', do you want to merge segments forcely ?',
    error_info: 'Failed to load external filters.',//alert of ExtFilterCtrl
    tip_to_delete_filter: 'Are you sure to delete ?',
    tip_delete_filter_part_one: 'Filter [',
    tip_delete_filter_part_two: '] has been deleted successfully!',
    tip_update_filter_part_two: '] updated successfully!',
    tip_create_filter_part_one: 'New Filter [',
    tip_create_filter_part_two: '] created successfully!',
    tip_to_resume_job: 'Are you sure to resume the job?',//alert of JobCtrl
    success_job_been_resumed: 'Job has been resumed successfully!',
    tip_to_discard_job: 'Are you sure to discard the job?',
    success_Job_been_discarded: 'Job has been discarded successfully!',
    tip_no_job_selected: 'No job selected.',
    error_failed_to_load_job: 'Failed to load job info, please check system log for details.',
    tip_invalid_model_json: 'Invalid model json format..',//alert of ModelEditCtrl
    tip_to_update_model: 'Are you sure to update the model?',
    tip_to_save_model: 'Are you sure to save the Model',
    tip_Please_note: 'Please note: if model schema is changed, all cubes of the model will be affected.',
    success_updated_model: 'Updated the model successfully.',
    success_created_model: 'Created the model successfully.',
    tip_to_drop_model: 'Are you sure to drop this model?',//alert of ModelCtrl
    success_model_drop_done: 'Model drop is done successfully',
    tip_select_target_project: 'Please select target project.',
    tip_to_clone_model: 'Are you sure to clone the model? ',
    success_clone_model: 'Clone model successfully',
    success_project_update: 'Project update successfully!',//alert of PageCtrl
    tip_new_project_created: 'New project created successfully!',
    tip_sure_to_delete: 'Are you sure to delete ?', //alert of ProjectContrl
    tip_project_delete_part_one: 'Project [',
    tip_project_delete_part_two: '] has been deleted successfully!',
    success_new_query_saved: 'New query saved..',
    tip_to_leave_page: 'You\'ve executing query in current page, are you sure to leave this page?',
    tip_select_project: 'Please select a project.',//alert of SourceMetaCtrl
    tip_to_synchronize: 'Please input table(s) you want to synchronize.',
    tip_choose_project: 'Please choose your project first!.',
    tip_result_unload_title: 'Failed',
    tip_result_unload_body: 'Failed to synchronize following table(s): ',
    success_table_been_synchronized: 'The following table(s) have been successfully synchronized: ',
    tip_partial_loaded_title: 'Partial loaded!',
    tip_partial_loaded_body_part_one: 'The following table(s) have been successfully synchronized: ',
    tip_partial_loaded_body_part_one: '\n\n Failed to synchronize following table(s):',
    tip_save_streaming_table: 'Are you sure to save the streaming table and cluster info ?',
    success_updated_streaming: 'Updated the streaming successfully.',
    tip_created_streaming: 'Created the streaming successfully.',
    tip_to_remove_cluster: 'Are you sure to remove this cluster ?',

}

});
