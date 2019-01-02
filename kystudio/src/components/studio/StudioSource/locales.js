export default {
  'en': {
    updateAt: 'Last update :',
    delete: 'Delete',
    dataLoad: 'Data Load',
    columns: 'Columns',
    sample: 'Sample',
    unloadTable: 'Are you sure to delete table?',
    unloadSuccess: 'Delete Table successfully.',
    remindLoadRange: 'If you have tables which increase by day, it is suggested to select the corresponding date column as partition key. Especially, tables containing historical data, in which new data is added into the newest partition.'
  },
  'zh-cn': {
    updateAt: '上次更新时间 :',
    delete: '删除',
    dataLoad: '数据加载信息',
    columns: '列级信息',
    sample: '样例数据',
    unloadTable: '确定要删除源表？',
    unloadSuccess: '删除成功',
    remindLoadRange: '如果有数据按时间递增的源表，建议选择一个合适的 日期列 作为源表的分区列。特别是那些包含历史数据的表，新的数据将会被加入最新的分区中。'
  }
}
