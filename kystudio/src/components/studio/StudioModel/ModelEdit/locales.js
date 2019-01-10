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
    saveSuccessTip: 'Model saved successfully. To enable it, you may need to add some indexes and load data into the model.',
    addIndexTip: 'Add Index',
    ignoreaddIndexTip: 'No Thanks',
    noDimensionTipContent: 'No dimension in the model. After the save, you can add table index for query on transaction resords. If the model needs to serve aggregate query,  we would suggest you to add some dimensions.',
    noDimensionAndMeasureTipContent: 'No dimension and measure in the model. After the save, you can add table index for query on transaction resords. If the model needs to serve aggregate query,  we would suggest you to add some dimensions and measures.',
    noDimensionTipTitle: 'Add Dimension',
    noDimensionAndMeasureTipTitle: 'Add Dimension and Measure',
    noDimensionGoOnSave: 'Save',
    noDimensionBackEdit: 'Back to edit'
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
    saveSuccessTip: '模型保存成功。为了真正启用本模型，您需要在模型中添加索引并向其加载数据。',
    addIndexTip: '添加索引',
    ignoreaddIndexTip: '不需要',
    noDimensionTipContent: '本模型中尚无维度，如此保存后将无法添加聚合索引。如果只需要明细查询，继续保存即可。如果本模型后续将服务于聚合函数的查询，系统建议您添加一些要分析的维度。',
    noDimensionTipTitle: '添加维度',
    noDimensionAndMeasureTipTitle: '添加维度和度量',
    noDimensionAndMeasureTipContent: '本模型中尚无维度和度量，如此保存后将无法添加聚合索引。如果只需要明细查询，继续保存即可。如果本模型后续将服务于聚合函数的查询，系统建议您添加一些要分析的维度和度量。',
    noDimensionGoOnSave: '继续保存',
    noDimensionBackEdit: '回到编辑'
  }
}
