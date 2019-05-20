export default {
  'en': {
    dialogTitle: 'Reload Table Metadata',
    reloadNoEffectTip: 'The source table {tableName} has no change on metadata. ',
    reloadEffectTip: 'The source table {tableName} has {changeChar}',
    reloadEffectTip1: ' The system will remove the columns and refresh {modelMode} automatically, details as below. ',
    addColumnsTip: ' {addedColumnsCount} column(s) added',
    reducedColumnsTip: ' {reducedColumnsCount} column(s) reduced',
    changedColumnsTip: 'the data type of {changedColumnsCount} column(s) changed',
    tableSample: 'Table Sampling',
    noEffectSampingTip: 'Sample job is still recommended to do because the table may have change on records. ',
    hasEffectSampingTip: 'Sample job is highly recommended to do because the table has change on metadata.',
    sampleCount: 'Sampling range should not exceed',
    rows: 'rows.',
    reloadSuccess: 'Successed to load source table {tableName}',
    invalidType: 'Please input an integer',
    invalidLarger: 'Input should be no larger than 20,000,000 rows',
    invalidSmaller: 'Input should be no less than 10,000 rows',
    modelchangeTip: '{modelCount} model(s) broken, edit the model(s) to add lost columns may fix;',
    dimChangeTip: '{dimensionCount} dimension(s)',
    measureChangeTip: '{measureCount} measure(s)',
    indexChangeTip: '{indexCount} index(es)',
    dimAndMeasureAndIndexChangeTip: '{changeChar} would be deleted;'
  },
  'zh-cn': {
    dialogTitle: '重载表',
    reloadNoEffectTip: '源表{tableName}元数据无变化。',
    reloadEffectTip: '重载的表 {tableName} {changeChar}',
    reloadEffectTip1: '系统将自动地从{modelMode}删除缺失列, 并刷新变化的{modelMode}。影响范围的细节如下。',
    addColumnsTip: '增加了 {addedColumnsCount} 列',
    reducedColumnsTip: '减少了 {reducedColumnsCount} 列',
    changedColumnsTip: '{changedColumnsCount} 列数据类型发生变化',
    tableSample: '表采样',
    noEffectSampingTip: '记录可能变化，推荐对该表进行数据抽样。',
    hasEffectSampingTip: '源表变化后，推荐对该表进行数据抽样。',
    sampleCount: '采样范围不超过',
    rows: '条。',
    reloadSuccess: '成功重载表 {tableName}',
    invalidType: '请输入一个整数',
    invalidLarger: '输入的值应不大于20,000,000行 ',
    invalidSmaller: '输入的值不小于10,000行',
    modelchangeTip: '{modelCount} 个模型元数据破损，请编辑模型尝试修复；',
    measureChangeTip: '{measureCount} 个度量',
    dimChangeTip: '{dimensionCount} 个维度',
    indexChangeTip: '{indexCount} 个索引',
    dimAndMeasureAndIndexChangeTip: '共有 {changeChar}被删除;'
  }
}
