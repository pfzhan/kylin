export default {
  'en': {
    'adddimension': 'Add Dimension',
    'addmeasure': 'Add Measure',
    'addjoin': 'Add Join Condition',
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
    'delTableTip': 'Once you delete the table, related dimensions, measures and joins will be deleted as well. Are you sure to delete them?',
    'noFactTable': 'Fact Table is mandatory for model',
    switchLookup: 'Switch to Lookup Table',
    switchFact: 'Switch to Fact Table',
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
    requiredName: 'Please input alias',
    modelDataNullTip: 'Failed to find this model!',
    saveSuccessTip: 'The model saved successfully and will be online. Add some indexes and load data into the model are highly recommended. A model won\'t be able to serve any query if it has no index and data.',
    addIndexTip: 'Add Index',
    ignoreaddIndexTip: 'No Thanks',
    noDimensionTipContent: 'No dimension in the model. After the save, you can add table index for query on transaction resords. If the model needs to serve aggregate query,  we would suggest you to add some dimensions.',
    noDimensionAndMeasureTipContent: 'No dimension and measure in the model. After the save, you can add table index for query on transaction resords. If the model needs to serve aggregate query,  we would suggest you to add some dimensions and measures.',
    noDimensionTipTitle: 'Add Dimension',
    noDimensionAndMeasureTipTitle: 'Add Dimension and Measure',
    noDimensionGoOnSave: 'Save',
    noDimensionBackEdit: 'Back to edit',
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
    searchInputPlaceHolder: 'Search table, dimension, measure, column name',
    delConnTip: 'Are you sure to delete this connection?',
    brokenEditTip: 'Please adjust the condition of error join(s) or partition column.',
    noTableTip: '<p>Add Table: drag the table from the left source tree and drop it to the central zone.</p><p>Add Join: drag the column from one table and drop it on another table.</p>',
    noBrokenLink: 'No error join(s).',
    canNotRepairBrokenTip: 'Sorry the broken model couldn\'t be recovered as its metadata has too many information lost. You can delete the model or contact support to get more information.'
  },
  'zh-cn': {
    'adddimension': '添加维度',
    'addmeasure': '添加度量',
    'addjoin': '添加连接条件',
    'editmeasure': '编辑度量',
    'editdimension': '编辑维度',
    'editjoin': '编辑连接条件',
    'showtable': '显示表',
    'tableaddjoin': '添加连接条件',
    'modelSetting': '模型设置',
    'userMaintainedModel': '手动模型',
    'systemMaintainedModel': '自动建模',
    'userMaintainedTip1': '系统无法更改模型的语义：维度、度量、表关联关系（Join tree）',
    'userMaintainedTip2': '系统可以更改模型下属的索引（聚合索引&明细表索引）',
    'userMaintainedTip3': '系统不能删除模型',
    'systemMaintainedTip1': '系统可以更改模型的语义：维度、度量、表关联关系（Join tree）',
    'systemMaintainedTip2': '系统可以更改模型下属的索引（聚合索引&明细表索引）',
    'systemMaintainedTip3': '系统可以删除模型',
    'avoidSysChange': '禁止系统修改模型语义',
    'allowSysChange': '允许系统修改模型语义',
    'delTableTip': '删除本表后，相关的维度、度量和连接关系都会被删除。确认要删除吗？',
    'noFactTable': '模型需要有一个事实表',
    switchLookup: '设置为维度表',
    switchFact: '设置为事实表',
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
    saveSuccessTip: '模型保存成功并将上线。系统建议您为模型添加索引并加载数据。没有索引和数据的模型，无法服务任何查询。',
    addIndexTip: '添加索引',
    ignoreaddIndexTip: '不需要',
    noDimensionTipContent: '本模型中尚无维度，如此保存后将无法添加聚合索引。如果只需要明细查询，继续保存即可。如果本模型后续将服务于聚合函数的查询，系统建议您添加一些要分析的维度。',
    noDimensionTipTitle: '添加维度',
    noDimensionAndMeasureTipTitle: '添加维度和度量',
    noDimensionAndMeasureTipContent: '本模型中尚无维度和度量，如此保存后将无法添加聚合索引。如果只需要明细查询，继续保存即可。如果本模型后续将服务于聚合函数的查询，系统建议您添加一些要分析的维度和度量。',
    noDimensionGoOnSave: '继续保存',
    noDimensionBackEdit: '回到编辑',
    searchHistory: '搜索历史',
    searchActionSaveSuccess: '{saveObj} 保存成功',
    measure: '度量',
    dimension: '维度',
    tableJoin: '表连接条件',
    addTableJoinCondition: '设置表连接条件',
    editTableJoinCondition: '设置表连接条件',
    addDimension: '添加维度',
    addMeasure: '添加度量',
    editDimension: '编辑维度',
    editMeasure: '编辑度量',
    searchTable: '搜索表',
    searchInputPlaceHolder: '搜索表名、维度、度量、列名',
    brokenEditTip: '请调整报错的关联条件或分区列。',
    delConnTip: '确认删除该连接关系吗？',
    noTableTip: '<p>添加表：从左侧数据源模块将表拖入中间区域。</p><p>建立关联：从一张表拖拽列到另一张表上。</p>',
    noBrokenLink: '没有需要修改的错误连线关系。',
    canNotRepairBrokenTip: '该模型丢失了太多的元数据信息，暂时无法进行恢复操作。您可以删除该模型或联系技术支持获取更多信息。'
  }
}
