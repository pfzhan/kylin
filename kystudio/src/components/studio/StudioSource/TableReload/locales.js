export default {
  'en': {
    dialogTitle: 'Reload Table Metadata',
    reloadNoEffectTip: 'The source table {tableName} has no change on metadata. ',
    reloadEffectTip: 'The source table {tableName} has {changeChar}',
    reloadEffectTip1: ' The system will remove the columns and update the index list',
    reloadEffectTip2: ' Details as below. ',
    sourceTable: 'The source table "{tableName}" has:',
    addColumnsTip: ' {addedColumnsCount} column(s) added',
    reducedColumnsTip: ' {reducedColumnsCount} column(s) reduced',
    dimensionTip: ' {delDimensionCount} dimension(s)',
    measureTip: ' {delMeasureCount} measure(s)',
    indexTip: ' {delIndexCount} index(es)',
    updateBaseIndexTip: '{}updateBaseIndexCount base index(es) would be updated',
    addLayoutTips: '{addLayoutsCount} index(es) would be added',
    afterLoaded: ' After being loaded, ',
    willBeDelete: ' would be deleted from the model(s) using this table.',
    dataTypeChange: ' {dataTypeChangeCount} column(s) changed the data type.',
    refreshCountTip: ' {refreshCount} index(es) would be refreshed and can serve queries after being rebuilt.',
    checkDetail: 'View Details',
    closeDetail: 'Hide Details',
    changedColumnsTip: 'the data type of {changedColumnsCount} column(s) changed',
    tableSample: 'Table Sampling',
    noEffectSampingTip: 'Sample job is still recommended to do because the table may have change on records. ',
    hasEffectSampingTip: 'Sample job is highly recommended to do because the table has change on metadata.',
    sampleCount: 'Sampling range should not exceed',
    rows: 'rows.',
    reloadSuccess: 'Succeeded to reload source table "{tableName}"',
    sampleSuccess: ' and submit the sampling job',
    structureSuccess: ' and building index task',
    concludingRemarks: ' which can be viewed on the Job page',
    invalidType: 'Please enter an integer',
    invalidLarger: 'Input should be no larger than 20,000,000 rows',
    invalidSmaller: 'Input should be no less than 10,000 rows',
    reloadTips: 'After being reloaded, ',
    modelchangeTip: '{modelCount} model(s) would be BROKEN and can\'t serve queries. You may repair the model(s) manually.',
    snapshotDelTip: 'The snapshot would be Broken and can\'t serve queries.',
    modelChangeAndSnapshotDel: '{modelCount} model(s) and the snapshot(s) would be BROKEN and can\'t serve queries.',
    indexCountChangeTip: '{indexCount} index(es) would be refreshed and would not be available for querying until being built;',
    addIndexTip: ' while {addIndexCount} index(es) would be added and would not be available for querying until being built.',
    dimChangeTip: '{dimensionCount} dimension(s)',
    measureChangeTip: '{measureCount} measure(s)',
    indexChangeTip: '{indexCount} index(es)',
    dimAndMeasureAndIndexChangeTip: '{changeChar} would be deleted',
    reloadBtn: 'Reload',
    reloadAndRefresh: 'Reload and Build',
    refreshIndexTip: ' and refresh the affected index(es)',
    rebuildLayout: 'which can serve queries after being rebuilt.'
  },
  'zh-cn': {
    dialogTitle: '重载表',
    reloadNoEffectTip: '源表 {tableName} 元数据无变化。',
    reloadEffectTip: '重载的表 {tableName} {changeChar}',
    reloadEffectTip1: '系统将自动地从 {modelMode} 删除缺失列，更新索引列表',
    reloadEffectTip2: '影响范围的细节如下：',
    sourceTable: '源表 "{tableName}" 中：',
    addColumnsTip: '增加了 {addedColumnsCount} 列',
    reducedColumnsTip: '减少了 {reducedColumnsCount} 列',
    dimensionTip: ' {delDimensionCount} 个维度',
    measureTip: ' {delMeasureCount} 个度量',
    indexTip: '{delIndexCount} 个索引',
    updateBaseIndexTip: '更新 {updateBaseIndexCount} 个基础索引',
    addLayoutTips: '增加了 {addLayoutsCount} 个索引',
    afterLoaded: '重载表后会从依赖该表的模型中删除',
    dataTypeChange: '{dataTypeChangeCount} 列的数据类型发生变化。',
    refreshCountTip: '{refreshCount} 个索引将被刷新，重新构建后才可服务于查询。',
    willBeDelete: '。',
    checkDetail: '查看详情',
    closeDetail: '收起详情',
    changedColumnsTip: '{changedColumnsCount} 列数据类型发生变化',
    tableSample: '表抽样',
    noEffectSampingTip: '记录可能变化，推荐对该表进行数据抽样。',
    hasEffectSampingTip: '源表变化后，推荐对该表进行数据抽样。',
    sampleCount: '抽样范围不超过',
    rows: '条。',
    reloadSuccess: '成功重载表 “{tableName}”',
    sampleSuccess: '抽样任务',
    and: '和',
    structureSuccess: '构建任务',
    concludingRemarks: '提交成功，可以在任务页面查看',
    invalidType: '请输入一个整数',
    invalidLarger: '输入的值应不大于 20,000,000 行 ',
    invalidSmaller: '输入的值不小于 10,000 行',
    reloadTips: '重载该表元数据后，',
    modelchangeTip: '{modelCount} 个模型将变为 BROKEN 状态，无法服务于查询。您可以手动编辑模型进行修复。',
    snapshotDelTip: '该表快照将变为 Broken 状态，无法服务于查询。',
    modelChangeAndSnapshotDel: '{modelCount} 个模型及其快照将变为 BROKEN 状态，无法服务于查询。',
    indexCountChangeTip: '{indexCount} 个索引将被刷新，构建后才可服务于查询；',
    addIndexTip: '{addIndexCount} 个索引将被添加，构建后才可服务于查询',
    measureChangeTip: '{measureCount} 个度量',
    dimChangeTip: '{dimensionCount} 个维度',
    indexChangeTip: '{indexCount} 个索引',
    dimAndMeasureAndIndexChangeTip: '共有 {changeChar} 被删除',
    reloadAndRefresh: '重载并构建索引',
    reloadBtn: '重载',
    refreshIndexTip: '并刷新受影响索引',
    rebuildLayout: '新增索引在构建后可服务于查询。'
  }
}
