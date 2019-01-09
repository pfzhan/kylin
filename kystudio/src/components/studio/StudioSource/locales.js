export default {
  'en': {
    updateAt: 'Last update :',
    delete: 'Delete',
    general: 'General',
    columns: 'Columns',
    sample: 'Sample',
    unloadTableTitle: 'Delete Source Table',
    unloadTable: 'Do you need to delete the table?',
    unloadSuccess: 'Delete Table successfully.',
    affactUnloadInfo: 'Delete the source table {tableName} will lead to removing related models and indexes, whose total storage size is {storageSize} MB.',
    remindLoadRange: 'If you have tables which increase by day, it is suggested to select the corresponding date column as partition key. Especially, tables containing historical data, in which new data is added into the newest partition.'
  },
  'zh-cn': {
    updateAt: '上次更新时间 :',
    delete: '删除',
    general: '基本信息',
    columns: '所有列',
    sample: '样例数据',
    unloadTableTitle: '删除源数据表',
    unloadTable: '确定要删除源表？',
    unloadSuccess: '删除成功',
    affactUnloadInfo: '删除源数据表 {tableName} 将删除所有依赖本表的模型和索引，总存储为 {storageSize}。',
    remindLoadRange: '如果有数据按时间递增的源表，建议选择一个合适的 日期列 作为源表的分区列。特别是那些包含历史数据的表，新的数据将会被加入最新的分区中。'
  }
}
