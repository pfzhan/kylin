export default {
  'en': {
    'adddimension': 'Add Dimension',
    'addmeasure': 'Add Measure',
    'addjoin': 'Add Join Relationship',
    'editmeasure': 'Edit Measure',
    'editdimension': 'Edit Dimension',
    'editjoin': 'Edit Join',
    'showtable': 'Show Table',
    'tableaddjoin': 'Add Join',
    'modelSetting': 'Model Setting',
    'userMaintainedModel': 'User Maintained Model',
    'systemMaintainedModel': 'System Maintained Model',
    'userMaintainedTip1': 'System is not able to change the model definition: dimension, measure or join tree',
    'userMaintainedTip2': 'System can change the model index: aggregate index or table index',
    'userMaintainedTip3': 'System is not able to delete this model',
    'systemMaintainedTip1': 'System can change the model definition: dimension, measure or join tree',
    'systemMaintainedTip2': 'System can change the model index: aggregate index or table index',
    'systemMaintainedTip3': 'System can delete this model',
    'avoidSysChange': 'Avoid system change semantics',
    'allowSysChange': 'Allow system change semantics',
    'delTableTip': 'All dimensions, measures and joins using this table would be deleted. Are you sure you want to delete this table?',
    'noFactTable': 'Please select a fact table.',
    switchLookup: 'Switch to Lookup Table',
    switchFact: 'Switch to Fact Table',
    kafakFactTips: 'Kafka tables can only be used as fact tables. Please delete the current fact table or switch it to a dimension table before loading a Kafka table.',
    kafakDisableSecStorageTips: 'Can\'t use Kafka tables when the tiered storage is ON.',
    editTableAlias: 'Edit table alias',
    deleteTable: 'Delete the table',
    noSelectJobs: 'Please check at least one item',
    add: 'Add',
    batchAdd: 'Batch Add',
    batchDel: 'Batch Delete',
    checkAll: 'Check All',
    unCheckAll: 'Uncheck All',
    delete: 'Delete',
    back: 'Back',
    requiredName: 'Please enter alias',
    modelDataNullTip: 'Can\'t find this model.',
    saveSuccessTip: 'The model has been saved successfully.',
    buildIndex: 'Build Index',
    createBaseIndexTips: 'Successfully added {createBaseNum} base index(es). ',
    updateBaseIndexTips: 'Successfully updated {updateBaseNum} base index(es).',
    addSegmentTips: 'To make it available for queries, please define the data range which this model would be served for.',
    addIndexTips: 'To improve query performance, please add and build indexes. ',
    addIndexAndBaseIndex: 'To improve query performance, more indexes could be added and built.',
    createAndBuildBaseIndexTips: '{createBaseIndexNum} base index(es) have been added successfully.',
    addIndex: 'Add Index',
    viewIndexes: 'View Index',
    addSegment: 'Add Segment',
    ignoreaddIndexTip: 'Not Now',
    noDimensionTipContent: 'No dimension has been added. If this model would be used for aggregate queries, please add some necessary dimensions for generating aggregate indexes later.',
    noDimensionAndMeasureTipContent: 'No dimension or measure has been added. If this model would be used for aggregate queries, please add some necessary dimensions and measures for generating aggregate indexes later.',
    noDimensionTipTitle: 'Add Dimension',
    noDimensionAndMeasureTipTitle: 'Add Dimension and Measure',
    noDimensionGoOnSave: 'Save',
    noDimensionBackEdit: 'Continue Editing',
    searchHistory: 'Searh History',
    searchActionSaveSuccess: '{saveObj} saved successfully',
    measure: 'Measure',
    dimension: 'Dimension',
    addTableJoinCondition: 'Add table join condition',
    editTableJoinCondition: 'Edit table join condition',
    tableJoin: 'Table join condition',
    addDimension: 'Add dimension',
    addMeasure: 'Add measure',
    editDimension: 'Edit dimension',
    editMeasure: 'Edit measure',
    searchTable: 'Search table',
    searchInputPlaceHolder: 'Search model\'s table alias, column, measure name or join relationship',
    delConnTip: 'Are you sure you want to delete this connection?',
    delConnTitle: 'Delete connection',
    brokenEditTip: 'This model is broken. Please check and adjust the join relationship, the partition column, and the filter condition.',
    noTableTip: '<p>Add Table: drag the table from the left source tree and drop it to the central zone.</p><p>Add Join: drag the column from one table and drop it on another table.</p>',
    noBrokenLink: 'No error join(s).',
    canNotRepairBrokenTip: 'Can\'t recover this model as too much metadata information is lost. Please contact technical support.',
    searchColumn: 'Search column name',
    modelChangeTips: 'You are modifying the model definition. After submitted, all indexes of this model may be rebuilt. The model will be unavailable to serve queries until the indexes are built successfully.',
    ignore: 'Ignore',
    saveAndSubmitJobSuccess: 'Successfully saved the changes, and submitted the job of loading data',
    tableHasOppositeLinks: 'A reserved join condition already exists between the tables. Please click on the join condition to modify.',
    changeTableJoinCondition: 'Modifying the table\'s type would affect the existing join condition. Please delete or modify the join condition first.',
    lockupTableToFactTableTip: 'Please add join condition from the fact table to a look up table.',
    noStarOrSnowflakeSchema: 'This join condition is not allowed in neither <a href="https://en.wikipedia.org/wiki/Star_schema" target="_blank">star</a> or <a href="https://en.wikipedia.org/wiki/Snowflake_schema" target="_blank">snowflake</a> schema. Please adjust and try again.',
    varcharSumMeasureTip: 'Can\'t save model. The following measures can\'t reference column(s) in Varchar type, as the selected function is SUM or PERCENTILE_APPROX.',
    measureRuleErrorTip: 'This measure\'s function ({type}) is incompatible with the referenced column, which is Varchar.',
    pleaseModify: 'Please modify.',
    iKnow: 'Got It',
    disabledConstantMeasureTip: 'Can\'t modify the default measure.',
    flattenLookupTableTips: 'Unable to use columns from this table for dimension and measure. Because the join relationship of this dimension table won\'t be precomputed.',
    disableDelDimTips: 'When the tiered storage is ON, the time partition column can\'t be deleted from the dimension.',
    forbidenCreateCCTip: 'Can\'t add computed column to fusion model',
    streamTips: 'For fusion model, the time partition column can\'t be deleted from the dimension.'
  },
  'zh-cn': {
    'adddimension': '添加维度',
    'addmeasure': '添加度量',
    'addjoin': '添加关联关系',
    'editmeasure': '编辑度量',
    'editdimension': '编辑维度',
    'editjoin': '编辑关联关系',
    'showtable': '显示表',
    'tableaddjoin': '添加关联关系',
    'modelSetting': '模型设置',
    'userMaintainedModel': '手动模型',
    'systemMaintainedModel': '自动建模',
    'userMaintainedTip1': '系统无法更改模型的语义：维度、度量、表关联关系（Join tree）',
    'userMaintainedTip2': '系统可以更改模型下属的索引（聚合索引&明细索引）',
    'userMaintainedTip3': '系统不能删除模型',
    'systemMaintainedTip1': '系统可以更改模型的语义：维度、度量、表关联关系（Join tree）',
    'systemMaintainedTip2': '系统可以更改模型下属的索引（聚合索引&明细索引）',
    'systemMaintainedTip3': '系统可以删除模型',
    'avoidSysChange': '禁止系统修改模型语义',
    'allowSysChange': '允许系统修改模型语义',
    'delTableTip': '删除本表后，相关的维度、度量和连接关系都会被删除。确认要删除吗？',
    'noFactTable': '模型需要有一个事实表',
    switchLookup: '设置为维度表',
    switchFact: '设置为事实表',
    kafakFactTips: 'Kafka 表只能作为事实表，如需替换请先将当前事实表设为维度表或删除。',
    kafakDisableSecStorageTips: '开启分层存储时无法使用 Kafka 表。',
    editTableAlias: '编辑别名',
    deleteTable: '删除本表',
    noSelectJobs: '请勾选至少一条',
    add: '增加',
    batchAdd: '批量增加',
    batchDel: '批量删除',
    checkAll: '全选',
    unCheckAll: '取消全选',
    delete: '删除',
    back: '返回',
    requiredName: '请输入别名',
    modelDataNullTip: '没有找到当前模型!',
    saveSuccessTip: '模型保存成功。',
    buildIndex: '构建索引',
    createBaseIndexTips: '成功添加 {createBaseNum} 个基础索引。',
    updateBaseIndexTips: '成功更新 {updateBaseNum} 个基础索引。',
    addSegmentTips: '为了使模型可服务于查询，请添加可服务的数据范围。',
    addIndexTips: '为了提高模型的查询效率，请添加索引并构建。',
    addIndexAndBaseIndex: '为了提高模型的查询效率，可继续添加索引并构建。',
    createAndBuildBaseIndexTips: '成功添加 {createBaseIndexNum} 个基础索引。',
    addIndex: '添加索引',
    viewIndexes: '查看索引',
    addSegment: '添加 Segment',
    ignoreaddIndexTip: '稍后添加',
    noDimensionTipContent: '模型尚未定义维度。若该模型后续将服务于聚合函数的查询，请添加分析所需相应维度，以供后续添加聚合索引。',
    noDimensionTipTitle: '添加维度',
    noDimensionAndMeasureTipTitle: '添加维度和度量',
    noDimensionAndMeasureTipContent: '模型尚未定义维度和度量。若该模型后续将服务于聚合函数的查询，请添加分析所需的维度和度量，以供后续添加聚合索引。',
    noDimensionGoOnSave: '继续保存',
    noDimensionBackEdit: '回到编辑',
    searchHistory: '搜索历史',
    searchActionSaveSuccess: '{saveObj} 保存成功',
    measure: '度量',
    dimension: '维度',
    tableJoin: '表关联关系',
    addTableJoinCondition: '设置表关联关系',
    editTableJoinCondition: '设置表关联关系',
    addDimension: '添加维度',
    addMeasure: '添加度量',
    editDimension: '编辑维度',
    editMeasure: '编辑度量',
    searchTable: '搜索表',
    searchInputPlaceHolder: '搜索模型的表名、列名、度量函数、关联关系等',
    brokenEditTip: '该模型已破损。请检查并调整关联关系、分区列、模型筛选条件。',
    delConnTip: '确认删除该连接关系吗？',
    delConnTitle: '删除连接关系',
    noTableTip: '<p>添加表：从左侧数据源模块将表拖入中间区域。</p><p>建立关联：从一张表拖拽列到另一张表上。</p>',
    noBrokenLink: '没有需要修改的错误连线关系。',
    canNotRepairBrokenTip: '该模型丢失了太多的元数据信息，暂时无法进行恢复操作。您可以删除该模型或联系技术支持获取更多信息。',
    searchColumn: '搜索列名',
    modelChangeTips: '您正在修改模型定义，提交后，可能会导致模型下的所有索引重新构建。索引构建完成前该模型不能服务于相关的查询。',
    ignore: '不再提示',
    saveAndSubmitJobSuccess: '保存成功，加载任务已提交',
    tableHasOppositeLinks: '两表之间已经存在一个反向关联关系，请点击连接关系进行修改。',
    changeTableJoinCondition: '修改该表的类型会影响现有的关联关系，请先删除或编辑连接关系。',
    lockupTableToFactTableTip: '请从事实表开始向维度表添加关联关系。',
    noStarOrSnowflakeSchema: '该连接不符合<a href="https://en.wikipedia.org/wiki/Star_schema" target="_blank">星型</a>或<a href="https://en.wikipedia.org/wiki/Snowflake_schema" target="_blank">雪花模型</a>的规范，请重新连接。',
    varcharSumMeasureTip: '无法保存模型。以下度量的函数类型 SUM 或 PERCENTILE_APPROX 不支持引用 Varchar 类型的列：',
    measureRuleErrorTip: '该度量的函数类型 {type} 不支持引用 Varchar 类型的列',
    pleaseModify: '请修改。',
    iKnow: '知道了',
    disabledConstantMeasureTip: '默认度量，暂不支持编辑和删除。',
    flattenLookupTableTips: '无法在维度和度量中使用该表中的列，因为该维度表的关联关系不进行预计算。',
    batchBuildSubTitle: '请为新增的索引选择需要构建至的数据范围。',
    disableDelDimTips: '在开启分层存储时，时间分区列不可从维度中删除。',
    forbidenCreateCCTip: '融合数据模型暂不支持添加可计算列',
    streamTips: '融合数据模型必须在维度中包含时间分区列。'
  }
}
