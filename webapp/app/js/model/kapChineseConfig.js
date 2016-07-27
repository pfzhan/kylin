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

KylinApp.constant('kapChineseConfig', {
  header:{
  chooseProject: '选择项目',
  login: '登陆',
  logout: '注销',
  help: '帮助',
  welcome: '欢迎',
  documents: [
    {link:'http://kyligence.io',displayName:'关于我们'}
  ]
  },
  /*
   module of  project
   */
  project:{
    /*
     part of manage project
     */
    info_no_result: '无相关结果',
    info_no_project: '没有项目',
    info_load_project: '加载项目...',
    theaditems: [
      {attr: 'name', name: '名称'},
      {attr: 'owner', name: '所有者'},
      {attr: 'description', name: '描述'},
      {attr: 'create_time', name: '创建时间'}
    ],
    action: '操作',
    manageProject: '项目管理',
    ac_project_edit: '编辑',
    ac_project_delete: '删除',
    cubes: 'Cube',   //project-cubes begin
    cubeName: 'Cube名称',
    cubeActions: '操作',
    ac_cube_detail: '详细',
    access: '权限',  //project-access begin
    ac_access_grant: '授权',
    ac_access_cancel: '删除',
    tip_access: '<div style="text-align: left"><label>什么是Cube权限</label><ul><li>CUBE QUERY: 查询Cube的权限</li><li>CUBE OPERATION: 构建Cube的权限，包括恢复和取消任务；包含查询权限.</li><li>CUBE MANAGEMENT: 编辑和删除Cube的权限，包含了构建Cube的权限.</li><li>CUBE ADMIN: 对Cube拥有所有权限.</li></ul></div>',
    accessName: '名称',
    plac_accessName: '用户帐号...',
    accessType: '类型',
    accessTypeUser: '用户',
    accessTypeRole: '群组',
    accessAccess: '权限',
    accessUpdate: '更新',
    accessPermission: '许可',
    chooses_accessUpdate_deaf:'-- 选择权限 -- ',
    chooses_accessUpdate: {
      READ: {name: 'Cube查询', value: 'READ', mask: 1},
      MANAGEMENT: {name: 'Cube编辑', value: 'MANAGEMENT', mask: 32},
      OPERATION: {name: 'Cube操作', value: 'OPERATION', mask: 64},
      ADMINISTRATION: {name: 'Cube管理', value: 'ADMINISTRATION', mask: 16}
    },
    accessRevoke: '取消',
    externalFilters: '其他过滤', //project-External Filters begin
    ac_externalFilters_filter: '过滤器',
    efNewFilter: '新的过滤器',
    efFilterName: '过滤器名称',
    plac_efFilterName: '可以使用字母、数字以及下划线',
    checkInfo_require_efFilterName: '过滤器名不可为空',
    checkInfo_invalid_efFilterName: '过滤器名不合法',
    efTableType: '表名',
    efResourcePath: 'Resource Path',
    plac_efResourcePath: '可以使用字母、数字以及下划线',
    checkInfo_require_efResourcePath: '"resource path"为必填项',
    checkInfo_invalid_efResourcePath: '"resource path"值不合法',
    efDescription: '描述',
    plac_efDescription: '过滤器描述..',
    ac_efClose: '关闭',
    ac_efSubmit: '提交',
    /*
     part of add project
     */
    addProject: '项目',
    addAdd: '添加项目',
    addTitle: '新的项目',
    addProjectName: '项目名称',
    plac_addProjectName: '可以使用字母、数字以及下划线',
    checkInfo_require_addProjectName: '项目名不可为空',
    checkInfo_invalid_addProjectName: '项目名不合法',
    addProjectDescription: '描述',
    plac_addProjectDescription: '项目描述..',
    addClose: '关闭',
    addSubmit: '提交',
    total:'总数' //sum of project
  },
  name: 'kylin',
  /*
   module of model
   */
  model:{
    /*
     management of model
     */
    model: '模型',
    schema:['模型信息','数据模型','维度','度量','设置'],
    models: '模型',
    theaditems : [
      {attr: 'name', name: '名称'},
      {attr: 'project', name: '项目'},
      {attr: 'fact_table', name: '事实表'},
      {attr: 'last_modified', name: '最后修改时间'}
    ],
    actions: '操作',
    info_no_model: '没有相关模型',
    info_create_model: '点击创建模型',
    next: '下一步',
    prev: '上一步',
    save: '保存',
    grid: 'grid',    //grid of model begin
    gridModelDesign: '模型设计',
    gridModelInfo: '模型信息',
    gridMIModelName: '模型名称',
    tip_title: 'gridMIModelName',
    tip_body_model_name: '<li>模型名称需要是所有项目中是唯一的.</li><li>模型创建后将不能重命名.</li>',
    check_info_required_model_name: '模型名不可为空.',
    check_info_invalid_model_name: '模型名不合法',
    gridMIDescription: '描述',
    check_info_warning_MI: '<div class="box box-danger"><div class="box-header with-border"> <i class="fa fa-warning"></i><h3 class="box-title">警告!</h3></div> <div class="box-body callout callout-warning"><ol><li>项目不可为空</li><li>模型名不可为空</li></ol></div></div>',
    gridDataModel: '数据模型',
    gridDMFactTable: '事实表',
    gridDMLookupTables: '维度表',
    gridDMNoLookupTables: '没有维度表',
    gridDMID: 'ID',
    gridDMTableName: '表名',
    gridDMJoinType: '连接类型',
    gridDMJoinCondition: '连接条件',
    gridDimensions: '维度',
    gridDSID: 'ID',
    gridDSTableName: '表名',
    gridDSColumns: '列',
    gridMeasure: '度量',
    gridMSID: 'ID',
    gridMSColumn: '列',
    gridMSSelectMeasure: '选择度量列',
    gridSettings: '设置',
    gridSTPartition: '分区',
    gridSTPartitionType: '分区类型',
    gridSTPartitionDate: '分区列（日期类型）',
    tip_title_gridSTPartitionDate: '用于分区的日期列',
    tip_gridSTPartitionDate: '<li>分区日期列是可选的；如果需要经常全量构建，请将分区日期列留空</Li><li>分区日期列中应该为日期值（类型可以是Date, Timestamp, String, VARCHAR）</li>',
    gridSTPartitionDateSelect: '--选择分区列--',
    gridSTPartitionDateFormat: '日期格式',
    gridSTPartitionDateFormatSelect: '--选择日期格式--',
    gridSTSetSeparate: '您使用单独的列来表示某天内的时间吗?',
    tip_tile_gridSTSetSeparate: '单独的时间列',
    tip_tile_gridHasSepTimeColumn: '分区时间列',
    tip_gridSTSetSeparate: '仅用于事实表上将日期和时间分开存储于两列的情况(KYLIN-1427)',
    gridHasSepTimeColumn: '分区列（时间类型）',
    tip_gridHasSeparateTimeColumn: '分区时间列中应该为时间值（类型可以是Timestamp, String, VARCHAR）',
    gridHasSepTimeColumnSelect: '--选择时间类型--',
    gridHasSepTimeColumnTF: '时间格式',
    gridHasSepTimeColumnTFSelect: '--选择时间格式--',
    gridSTFilter: '过滤器',
    plac_gridSTFilter: '请输入WHERE条件，不需要输入\'WHERE\'',
    //gridSTWhere: 'Where',
    edit: '编辑',  //edit of model begin
    editDMSelectFact: '-- 选择事实表 --',
    editDMAddLookupTable: '添加查找表',
    editDMSelectLookup: '-- 选择查找表 --',
    editDMActions: '操作',
    editDMLookupAdd: '添加',
    editDMLookupAdd: '编辑',//查找表 modal
    editDMLookupTableName: '查找表名称',
    editDMLookupNewJoin: '新的连接类型',
    tip_join_delete: '删除',
    tip_header_lookup_modal: ' <h4 class="box-title">提示</h4>',
    tip_body_lookup_modal: '<ol class="text-info"><li>清选择查找表</li><li>指定事实表和查找表的连接关系.</li><li>连接类型需跟SQL查询时的连接关系相同</li></ol>',
    ac_lookup_ok: '确定',
    ac_lookup_cancel: '取消',
    clone: '克隆',
    drop: '删除',
    visualization: '可视化'
  },
  cube:{
    cubeName: 'Cube名称',
    tip_cube_required: 'Cube名称不可为空.',
    tip_cube_name_invalid: 'Cube名称不合法.',
    cubeCubeDesigner: 'Cube设计器',
    cubeSave: '保存',
    cubeNext: '下一步',
    cubePrev: '上一步',
    cubesIn: '-- 所有 --',
    ac_cube: 'Cube',
    theaditems: [
      {attr: 'name', name: '名称'},
      {attr: 'detail.model_name', name: '模型'},
      {attr: 'status', name: '状态'},
      {attr: 'size_kb', name: 'Cube大小'},
      {attr: 'input_records_count', name: '源数据条目'},
      {attr: 'last_build_time', name: '最后构建时间'},
      {attr: 'owner', name: '所有者'},
      {attr: 'create_time_utc', name: '创建时间'}
    ],
    actions: '操作',
    schema: ['Cube信息','维度','度量','更新配置','高级设置','配置覆盖','概览'],
    admins: 'Admins',
    storage: '存储',
    streaming: 'Streaming',
    cubeActions:['删除','编辑','构建','刷新','合并','禁用','启用','清理','克隆'],
    edit: '编辑',
    SegmentId: 'Segment ID',
    cubeItems: ['Grid','SQL','JSON(Cube)','权限','通知列表','存储'],
    cubeNoNotification: '通知列表(逗号分隔)',
    ac_cubeNotification_save: '保存',
    cubeHBHTable: 'HTable:',
    cubeHBRegionCount: 'Region数量:',
    cubeHBSize: '大小:',
    cubeHBStartTime: '开始时间:',
    cubeHBEndTime: '结束时间:',
    cubeHBTotalSize:  '总大小:',
    cubeHBTotalNumber: '总个数:',
    cubeHBNoHBaseInfo: '没有HBase相关信息.',
    cubeCIModelName: '模型名称',
    cubeCIAddChooseModel: '-- 选择模型 --',
    tip_title_cube_name: 'Cube名称',
    tip_body_cube_name: '<li>Cube名称需要是全系统唯一的</li><li>创建后不能修改Cube名称</li>',
    cubeCINotifiEmail: '通知邮件列表',
    cubeCINotifiEvents: '需通知的事件',
    tip_body_cubeCINotifiEvents: 'ERROR状态总是会触发一条通知',
    cubeCIDescription: '描述',
    cubeDSAddDS: '添加维度',
    cubeDSNormal: '普通维度',
    cubeDSDerived: '可推导维度',
    cubeDSSwitchDer: 'derived',
    cubeDSColumn: 'Column',
    cubeDSSwitchNormal: 'normal',
    cubeDSAutoGenerator: '批量自动生成',
    cubeDSID: 'ID',
    cubeDSName: '名称',
    cubeDSTableName: '表名',
    cubeDSType: '类型',
    cubeDSActions: '操作',
    info_no_following_columns: '下面的这些列，被定义在列多个维度中. <small>不建议这么做</small>',
    cubeDSEditDSEdit: '编辑维度',
    cubeDSEditDSAdd: '添加维度',//add dimension begin
    tip_no_dimension_name: '维度名不可为空.',
    tip_derived_dimension: '可推导(Derived)维度需要来自查找表.',
    cubeDSColumnName: '列名',
    cubeDSSelectColumn: '-- 选择列 --',
    ac_delete_cubeDSColumn: '删除',
    cubeDSNewHierarchy: '新建层级维度',
    tip_cubeDSNewHierarchy: '通过拖拉以排序.',
    cubeDSNewDerived: '新建可推导维度',
    tip_title_cubeDS: '提示',
    tip_body_cubeDS: ' <li>在输入框中输入，系统会自动提示</li><li>请先从星型模型中选择事实表</li><li>数据类型需要与Hive表中的类型相一致</li><li>连接类型需跟SQL查询时的连接关系相同</li><li>Using Derived for One-One relationship between columns, like ID and Name</li>',
    ac_ok_cube: '确定',
    ac_cancel_cube: '取消',
    cubeDSAutoGenerate: '自动生成维度 <small>这个工具会帮助您批量生成维度.</small> ',//Auto Generate Dimensions begin
    cubeDSColumns: 'Columns',
    cubeDSFactTable: '[事实表]',
    cubeDSLookupTable: '[查找表]',
    tip_title_DSAutoGenerate: '自动生成规则',
    tip_body_DSAutoGenerate: '<li>如果某列已经被创建为维度，将不能再选择此列.</li><li>事实表上的每个选中的列，会自动创建为一个普通维度.</li><li>查找表上选中的列，会被自动创建为从外键可推导的(Derived)维度.</li>',
    cubeMSName: '名称',//measures begin
    cubeMSExpression: '表达式',
    cubeMSParameters: '参数',
    cubeMSParamType: '参数类型',
    cubeMSParamValue: '参数值',
    tip_cubeMSParamValue: '<li>当参数类型是"Column"时，只能选择一个列做为值</li><li>基于HyperLogLog的Distinct Count是近似计算，请选择能接受的误差率；低误差率意味着更多的存储空间和更长的构建时间。</li>',
    cubeMSParamValueSelect: '-- 选择列 --',
    cubeMSConstant: '常量',
    cubeMSReturnType: '返回类型',
    cubeMSActions: '操作',
    cubeMSEditMS: '编辑度量',
    cubeMSMeasureName: '度量名称..',
    tip_cubeMSExpression: 'Cube必须有一个Count(1)的度量, 并使用"_Count_"做为度量名(已经自动生成)',
    cubeMSType: '类型',
    cubeMSValue: '值',
    cubeMSGB: 'Group By列',
    cubeMSOK: '确定',
    cubeMSCancel: '取消',
    cubeMSParameterAdd: '添加参数',
    cubeMSParameterSelect: '选择Group By列',
    cubeRSAutoMerge: '触发自动合并的时间阀值',//refresh_settings begin
    tip_cubeRSAutoMerge: 'The thresholds will be checked ascendingly to see if any consectuive segments\' time range has exceeded it. For example the [7 days, 30 days] will result in daily incremental segments being merged every 7 days, and the 7-days segments will get merged every 30 days.',
    cubeRSNewThresholds: '新建阀值',
    cubeRSRetentionThreshold: '保留时间阀值',
    tip_cubeRSRetentionThreshold: 'By default it\'s \'0\',which will keep all historic cube segments ,or will keep latest [Retention Threshold] days cube segments.',
    tip_cubePartitionDate: '选择起始日期（如果此模型是按日期分区的）',
    cubeASID: 'ID',//Advanced Setting begin
    cubeASAggregationGroups: '聚合组',
    cubeASIncludes: '包含的维度',
    cubeASMandatoryDimensions: '必需维度',
    plac_cubeAS: '选择列',
    cubeASHierarchyDimensions: '层级维度',
    cubeASNewHierarchy: '新的层级',
    cubeASJointDimensions: '组合维度',
    cubeASNewJoint: '新的组合',
    cubeASNewAggregationGroup: '新建聚合组',
    cubeASRowkeys: 'Rowkey',
    tip_title_cubeASRowkeys: 'Rowkey',
    tip_body_cubeASRowkeys: '<h4>是否按该列分散存储?</h4><p>若设为"true"，Cube数据将按该列值分散存储</p><h4>Rowkey编码</h4><ol><li>选用"dict"编码将该维度构建字典保存</li><li>选用"fixed_length"编码将该维度保存为等长字节块</li><li>选用"int"编码将该维度以整数编码保存</li></ol>',
    cubeASID: 'ID',
    cubeASColumn: '列',
    cubeASEncoding: '编码',
    cubeASLength: '长度',
    cubeASShardBy: 'Shard By',
    tip_rowkey_column_name: 'rowkey列名..',
    tip_rowkey_column_length: 'rowkey列长度..',
    tip_false_by_default: '默认为false',
    tip_Column_Name: '列名..',
    cubeASNewRowkeyColumn: '新加Rowkey列',
    plac_cubeCOKey: 'Key',//Configuration Overwrites begin
    tip_cubeCOKey: '属性值不能为空.',
    plac_cubeCOValue: 'Value',
    tip_cubeCOValue: '属性值不能为空.',
    cubeCOProperty: 'Property',
    cubeCOTips: '提示',
    tip_cubeCOTips: 'Cube级的属性值将会覆盖kylin.prperties中的属性值',
    cubeOVModelName: '模型名称',//Overview begin
    cubeOVCubeName: 'Cube名',
    cubeOVFactTable: '事实表',
    cubeOVLookupTable: '查找表',
    cubeOVDimensions: '维度',
    cubeOVMeasures: '度量',
    cubeOVDescription: '描述',
    tip_loading_cubes: '下载Cubes...'
  },
  monitor:{
    monitor: '监控',
    jobs: '任务',
    slowQueries: '过慢的查询',
    jobsCubeName: 'Cube名称:',
    plac_jobsCubeName: '过滤器 ...',
    jobsJobsIn: 'Jobs in',
    jobConfig:{},//引用jobConfig.js
    tip_no_job: '没有任务记录',
    tip_loading_jobs: '加载任务...',
    jobResume: '恢复',
    jobsDetailInfo: '详细信息',//job detail begin
    tip_jobsDetailInfo: '收起',
    jobsJobName: '任务名',
    jobsJobID: 'ID',
    jobsStatus: '状态',
    jobsDuration: '持续时间',
    jobsMapReduceWaiting: 'MapReduce等待时间',
    jobsStart: '开始',
    jobsStepName: '步骤名:',
    jobsDataSize: '数据大小:',
    tip_Parameters: '参数',
    tip_Log: '日志',
    tip_MRJob: 'MR任务',
    jobsEnd: '结束',
    tip_no_job_selected: '请选择一个任务.',
    jobsModalParameters: '参数',
    jobsModalOutput: '输出',
    jobsModalLoading: '下载中 ...',
    jobsModalCmdOutput: 'cmd_output',
    jobsModalClose: '关闭',
    jobsModalSubmit: '上传',
    jobsCubeMergeConfirm: 'CUBE MERGE CONFIRM',//jobs merge modal begin
    jobsNOSegment: 'NO SEGMENT TO MERGE.',
    jobsPartitionDate: 'PARTITION DATE COLUMN',
    jobsMergeStart: 'MERGE START SEGMENT',
    jobsMergeEnd: 'MERGE END SEGMENT',
    jobsSegmentDetail: 'START SEGMENT DETAIL',
    jobsStartDate: '起始日期 (包含)',
    jobsEndDate: '结束日期 (不包含)',
    jobsLastbuildTime: '上次构建时间',
    jobsLastbuildID: '上次构建ID',
    jobsEndSegment: 'END SEGMENT DETAIL',
    tip_cube_segment: '此Cube已经有一个构建任务，它可能还在运行，或者运行出错，请检查. 如果你需要运行一个新的构建任务，请等待当前任务运行结束，或者丢弃它.',
    tip_no_partition: '没有定义分区日期列.',
    jobsCubeRefreshConfirm: 'CUBE REFRESH CONFIRM',//jobs refresh modal begin
    jobsNoSegmentRefresh: '没有可以刷新的Segment.',
    jobsPartitionDateColumn: '分区日期列',
    jobsRefreshSegment: '刷新的SEGMENT',
    jobsSegmentDetail: 'SEGMENT详细信息',
    jobsCubeBuildConfirm: 'Cube构建确认',//jobs submit modal begin
    tip_no_project_selected: '没有选择项目.',//job slow query begin
    tip_no_slow_query: '没有记录到过慢的查询.',
    tip_loading_queries: '加载查询记录...',
  },
  insight:{
    insight: '洞察',
    tables: '表',//query.html begin
    tip_no_table: '没有任何表.',
    tip_loading_Tables: '下载中...',
    newQuery: '新查询',
    savedQueries: '保存的查询',
    queryHistory: '查询历史',
    tip_list_tables: '技巧: Ctrl+Shift+Space 或 Alt+Space(Windows), Command+Option+Space(Mac) 可以在查询框中列出表/列名.',
    project: '项目:',
    limit: 'LIMIT',
    ac_submit: '递交',
    sqlName: 'SQL 名称:',
    description: '描述',
    ac_resubmit: '重新递交',
    ac_remove: '移除',
    tip_no_query_history: '没有查询历史.',
    queriedAt: '查询时间: ',
    inProject: '所在项目:',
    status: '状态:',
    tip_no_query_result: '没有查询结果.',
    tip_no_saved_query: '没有保存的查询',
    startTime: '开始时间:',//query detail begin
    duration: '持续时间: ',
    ac_return: '返回',
    ac_save: '保存',
    queryString: '查询',
    queryStatus: ['Failed','Success','Executing...'],
    cubes: 'Cube',
    results: '查询结果',
    tip_curQuery_result_partial: '!注意: 当前的结果集是不完整的；请点击 \'显示所有记录\' 按钮来获取所有结果.',
    showAll: '显示所有记录',
    visualization: '可视化',
    grid: 'Grid',
    ac_export: '导出',
    tip_loading: '加载中...',
    tip_more: '更多',
    graphType: '图形类型',
    dimensions: '维度',
    chooseDimension: '-- 选择维度 --',
    metrics: '度量',
    plac_select_metrics: '选择度量..',
    chooseMetrics: '-- 选择度量 --',
    tip_no_graph_generated: '没有生成图形.',
    saveQuery: '保存查询', //Save Query Modal begin
    project: '项目',
    name: '名称',
    plac_name: '名称..',
    plac_description: '描述..',
    ac_close: '关闭'
  },
  data_source:{
    data_source: '数据源',
    tables: '表',//--table tree begin
    tip_load_hive_table: '加载Hive表',
    tip_load_table_tree: '使用图形化树状结构方式加载Hive表',
    tip_unLoad_hive_table: '卸载Hive表',
    tip_add_streaming_table: '新建流式表',
    titleAddHiveTable: '加载Hive表元数据',//modal addHiveTable begin
    project: '项目:',
    tableNames: '表名:(逗号分隔)',
    plac_table_with_comma: 'table1,table2  默认系统会使用\'Default\'做为数据库名, 可以指定数据库名如 \'database.table\'',
    sync: '同步',
    cancel: '取消',
    titleAddHiveTableFromTree: '使用树状结构加载Hive表',//modal addHiveTableFromTree begin
    plac_filter: '过滤器',
    tip_loading_databases: '加载数据库...',
    tip_loading_tables: '加载中...',
    ac_show_more: '更多',
    ac_show_all: '所有',
    titleRemoveHiveTable: '卸载Hive表',//modal removeHiveTable begin
    titleAddStreamingSource: '流式表和集群信息',//modal addStreamingSource begin
    prev: '上一步',
    next: '下一步',
    submit: '递交',
    //part of modal addStreamingSource-- loadStreamingTable  --step 1
    info_loadStreamingTable: ' 请在此输入一条流数据(json)范例，系统将根据它来检测列名和属性，创建表模式。',
    tip_streaming_sourceSchema: '输入一条流数据范例以创建表模式.',
    tip_table_schemaChecked: '非法的json数据格式, 请检查并重试.',
    tip_table_schemaChecked_detail: '<li>选择一个“timestamp”类型的列.</li><li>默认系统会使用\'Default\'做为数据库名, 可以指定数据库名如 \'database.table\'</li><li>将从此“timestamp”列推导其它时间列，以帮助用户按不同时间粒度分析数据.</li>',
    tableName: '表名',
    tip_table_name: '表名不可为空.',
    column: '列',
    columnType: '列类型',
    comment: '注释',
    plac_select_column_type: '选择列类型',
    timestamp: 'timestamp',
    derivedTimeDimension: '推导的时间维度',
    tip_choose_timestamp: '至少选择一个\'timestamp\' 类型的列.',
    tableSchema: '表结构:',//--table detail begin
    columns: '列',
    extendInforFDFmation: '扩展信息',
    streamingCluster: '流集群',
    name: '名称',
    hiveDatabase: 'Hive数据库',
    snapshotTime: '快照时间',
    location: '位置',
    inputFormat: '输入格式',
    outputFormat: '输出格式',
    owner: '所有者',
    totalFileNumber: '总文件数',
    totalFileSize: '总文件大小',
    partitioned: '是否分区',
    partitionColumns: '分区列',
    plac_column: '过滤器 ...',
    theaditems: [
      {attr: 'id', name: 'ID'},
      {attr: 'name', name: '名称'},
      {attr: 'datatype', name: '数据类型'},
      {attr: 'cardinality', name: '基数'}
    ],
    tip_no_table_selected: '没有表被选中.',
    noTables: '没有表',
    tip_load_hive_table: '点击下载Hive中的表',
    tip_no_project_selected: '没有项目被选中',
    tip_select_project: '请先选中项目',
    tip_select_table: '请选择表'
  },
  system:{
    system: '系统',
    serverConfig: '服务器配置',
    serverEnvironment: '服务器环境',
    actions: '操作',
    reloadMetadata: '重载元数据',
    tip_reloadMetadata: '重载元数据并清理缓存',
    calculateCardinality: '计算基数',
    tip_calculateCardinality: '输入表名，计算各列的基数',
    cleanUpStorage: '清理存储空间',
    tip_cleanUpStorage: '清理无用的HDFS和HBase存储空间',
    disableCache: '禁用缓存',
    enableCache: '启用缓存',
    tip_enableCache: '启用查询缓存',
    setConfig: '设置配置',
    tip_setConfig: '更新服务器配置',
    diagnosis: '诊断',
    tip_diagnosis: '为当前项目创建并下载诊断包',
    links: '链接',
    loadHiveTable: '加载Hive表元数据',//modal calCardinality begin
    tableName: '表名:',
    plac_tableName: 'table1,table2',
    delimiter: '分隔符:',
    valueDeliRadios: 'default -- 系统会从Hive读取格式信息，如果你不确定，请选择"default".',
    formatRadios: 'default -- 系统会从Hive读取格式信息，如果你不确定，请选择"default".',
    text: 'text',
    sequence: 'sequence',
    sync: '同步',
    cancel: '取消',
    updateConfig: '更新配置',//modal updateConfig begin
    key: 'Key',
    tip_key: 'Config Key..',
    tip_required_key: 'Key不能为空.',
    value: 'Value',
    tip_value: '配置值..',
    tip_required_value: 'Value不能为空.',
    update: '更新'
  },
  alert:{
    error_info: '发生了错误.',
    success_grant: '权限已授予', //alert of AccessCtrl
    userNotFound: '未找到用户',
    tip_sure_to_revoke: '确定要取消权限?',
    success_access_been_revoked: '权限已取消.',
    tip_to_reload_metadata: '确定要重载元数据并清理缓存?',//alert of AdminCtrl
    success_server_environment: '成功获取环境信息',
    success_server_config: '成功获取服务器配置',
    success_cache_reload: '缓存已清理',
    tip_clean_HDFS_HBase: '确定要清理无用的HDFS和HBase空间?',
    success_storage_cleaned: '存储清理任务已触发.',
    success_cache_disabled: '缓存已禁用',
    tip_enable_query_cache: '确定要开启查询缓存?',
    success_cache_enabled: '缓存已启用',
    success_cardinality_job: '计算基数的任务已递交，稍后请刷新页面.',
    success_config_updated: '配置更新成功',
    tip_no_project_selected: '没有被选中的项目.',
    tip_to_disable_cache: '确定要禁用查询缓存?',
    error_failed_to_load_query: '发生了错误.',//alert of BadQueryCtrl;
    success_notify_list_updated: '通知列表已更新.',//alert of CubeCtrl
    tip_select_project: '请先选择项目.',//alert of CubeEditCtrl
    tip_invalid_cube: '无效的Cube json格式...',
    tip_to_save_cube: '确定要保存Cube?',
    success_updated_cube: '更新Cube成功',
    success_created_cube: '创建Cube成功.',
    tip_column_required: '[TOP_N]的Group by列不能为空',//alert of CubeMeasuresCtrl
    tip_remove_lookup_table: '删除后，所有与此表相关的维度会一起删除，确定删除查找表?',//alert of CubeModelCtrl
    tip_no_cube_detail: '没有Cube详细信息.',//alert of cubesCtrl
    tip_no_cube_detail_loaded: '没有Cube详细信息.',
    tip_sure_to_enable_cube: '请注意，如果在禁用期间，Cube的元数据发生改变，所有的Segment会被丢弃. 确定要启用Cube?',
    success_Enable_submitted: 'Cube已启用',
    tip_to_purge_cube: '确定要清空此Cube?',
    success_purge_job: 'Cube已清空',
    tip_disable_cube: '确定要禁用此Cube? ',
    success_disable_job_submit: 'Cube已禁用',
    tip_metadata_cleaned_up: '删除后, Cube定义及数据会被清除，且不能恢复. ',
    tip_cube_drop: 'Cube已删除',
    tip_to_start_build: '确定要开始构建?',
    success_rebuild_job: '构建任务已提交',
    tip_select_target_project: '选择项目.',
    tip_to_clone_cube: '确定要克隆Cube? ',
    success_clone_cube: 'Cube已克隆',
    success_rebuild_job: '构建任务已提交',
    tip_rebuild_part_one: '发现空的Segment',
    tip_rebuild_part_two: ', 要强制合并空的Segment吗 ?',
    error_info: '加载外部过滤器失败.',//alert of ExtFilterCtrl
    tip_to_delete_filter: '确定要删除?',
    tip_delete_filter_part_one: '过滤器 [',
    tip_delete_filter_part_two: '] 删除成功!',
    tip_update_filter_part_two: '] 更新成功!',
    tip_create_filter_part_one: '过滤器 [',
    tip_create_filter_part_two: '] 创建成功!',
    tip_to_resume_job: '确定要恢复任务?',//alert of JobCtrl
    success_job_been_resumed: '任务已恢复',
    tip_to_discard_job: '确认要抛弃任务?',
    success_Job_been_discarded: '任务已抛弃',
    tip_no_job_selected: '没有选择任务.',
    error_failed_to_load_job: '加载任务信息失败，请检查系统日志.',
    tip_invalid_model_json: '非法的模型json格式..',//alert of ModelEditCtrl
    tip_to_update_model: '确定要更新模型?',
    tip_to_save_model: '确定要保存模型？',
    tip_Please_note: '请注意: 模型的更改，会影响所有使用它的Cube.',
    success_updated_model: '更新模型成功.',
    success_created_model: '创建模型成功.',
    tip_to_drop_model: '确定要删除模型?',//alert of ModelCtrl
    success_model_drop_done: '模型删除成功',
    tip_select_target_project: '请选择项目.',
    tip_to_clone_model: '确定要克隆模型? ',
    success_clone_model: '模型克隆成功',
    success_project_update: '项目更新成功',//alert of PageCtrl
    tip_new_project_created: '项目创建成功',
    tip_sure_to_delete: '确定删除?', //alert of ProjectContrl
    tip_project_delete_part_one: '项目 [',
    tip_project_delete_part_two: '] 删除成功',
    success_new_query_saved: '查询已保存',
    tip_to_leave_page: '正在此页面执行查询，确定要离开?',
    tip_select_project: '请选择项目.',//alert of SourceMetaCtrl
    tip_to_synchronize: '输入要同步的表名.',
    tip_choose_project: '请先选择项目.',
    tip_result_unload_title: '失败',
    tip_result_unload_body: '同步下列表时发生错误: ',
    success_table_been_synchronized: '下列表的信息已成功同步: ',
    tip_partial_loaded_title: '部分同步',
    tip_partial_loaded_body_part_one: '下列表的信息已成功同步: ',
    tip_partial_loaded_body_part_one: '\n\n 同步失败的表是:',
    tip_save_streaming_table: '确定要保存此流式表及其集群信息?',
    success_updated_streaming: '成功更新流式表.',
    tip_created_streaming: '成功创建流式表.',
    tip_to_remove_cluster: '确定要删除此集群信息?',

  }

});
