export default {
  'en': {
    updateAt: 'Last update :',
    delete: 'Delete',
    reload: 'Reload',
    general: 'Storage',
    columns: 'Columns',
    sample: 'Sample',
    sampling: 'Sample Data',
    unloadTableTitle: 'Delete Source Table',
    unloadTable: 'Do you need to delete the table {tableName}?',
    unloadSuccess: 'Delete Table successfully.',
    affactUnloadInfo: 'The source table {tableName} is referenced by a running job. Please confirm whether to delete this table and discard the running job.',
    dropTabelDepen: 'This source table {tableName} is referenced by models, whose total storage size is {storageSize}. Please confirm whether to only delete this table or delete all.<br/><br/>Deleting Table: Only deleting this source table and stopping related jobs and this will cause models (indexes) being broken. You can load this table again to fix.<br/><br/>Deleting Model: Deleting this source table and referenced models (indexes) as well as stopping related jobs.',
    remindLoadRange: 'If you have tables which increase by day, it is suggested to select the corresponding date column as partition key. Especially, tables containing historical data, in which new data is added into the newest partition.',
    remindLoadRangeTitle: 'Add Partition',
    sampleDesc: 'The system will sample the table {tableName}.',
    sampleDesc1: 'Sampling range should not exceed ',
    sampleDesc2: ' rows.',
    sampleDialogTitle: 'Table Sampling',
    invalidType: 'Please input an integer',
    minNumber: 'Input should be no less than 10,000 rows',
    maxNumber: 'Input should be no larger than 20,000,000 rows',
    confirmSampling: 'The source table {table_name} has a related sample job running, it will be discarded if you re-submit a new sample job. Do you really want to re-submit a new one?',
    samplingTableJobBeginTips: 'Sampling job for table [{tableName}] has been submitted successfully, you can view the job progress in the Monitor page.',
    deleteAll: 'Delete All',
    deleteTable: 'Delete Table'
  },
  'zh-cn': {
    updateAt: '上次更新时间 :',
    delete: '删除',
    reload: '重载',
    general: '存储信息',
    columns: '所有列',
    sample: '抽样',
    sampling: '抽样数据',
    unloadTableTitle: '删除源数据表',
    unloadTable: '确定要删除源表 {tableName}？',
    unloadSuccess: '删除成功',
    affactUnloadInfo: '源表 {tableName} 正在被运行的任务引用，请确认是否删除该表并终止对应任务。',
    dropTabelDepen: '该源表 {tableName} 正在被模型和索引引用，总存储为 {storageSize}。请确认仅删除该表或全部删除<br/><br/>删除源表：仅删除源表信息，并停止相关任务。删除后将导致对应模型变为不可用状态，您可以通过再次添加该表进行修复。<br/><br/>全部删除：删除源表及其相关模型（索引组），并停止相关任务。',
    remindLoadRange: '如果有数据按时间递增的源表，建议选择一个合适的日期列作为源表的分区列。特别是那些包含历史数据的表，新的数据将会被加入最新的分区中。',
    remindLoadRangeTitle: '添加分区列',
    sampleDesc: '系统将对表 {tableName} 进行全表抽样。',
    sampleDesc1: '数据抽样范围不超过',
    sampleDesc2: '行。',
    sampleDialogTitle: '表级数据抽样',
    invalidType: '请输入一个整数',
    minNumber: '输入值应不小于 10,000 行',
    maxNumber: '输入值应 不大于 20,000,000 行',
    confirmSampling: '表 {table_name} 有正在进行的抽样任务，再次触发会终止前一个抽样任务。您确认要重新抽样吗？',
    samplingTableJobBeginTips: '表 [{tableName}] 抽样任务提交成功，您可以在监控页面查看任务进度。',
    deleteAll: '全部删除',
    deleteTable: '删除源表'
  }
}
