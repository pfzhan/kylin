export default {
  'en': {
    viewAll: 'View all',
    custom: 'Custom',
    recommended: 'Recommended',
    tableIndexTitle: 'Index ID {indexId}',
    delIndexTip: 'Are you sure you want to delete this index?',
    delIndex: 'Delete Index',
    includeColumns: 'Columns',
    aggTableIndexTips: 'No table indexes are defined. Please add dimensions that would be used in detail queries into indexes.',
    tableIndex: 'Table Index',
    addTableIndex: 'Add Table Index',
    cardinality: 'Cardinality',
    indexTimeRange: 'Index’s Time Range',
    refuseAddIndexTip: 'Can\'t add streaming indexes. Please stop the streaming job and then delete all the streaming segments.',
    refuseRemoveIndexTip: 'Can\'t delete streaming indexes. Please stop the streaming job and then delete all the streaming segments.',
    refuseEditIndexTip: 'Can\'t edit streaming indexes. Please stop the streaming job and then delete all the streaming segments.'
  },
  'zh-cn': {
    viewAll: '查看所有',
    custom: '自定义',
    recommended: '系统推荐',
    tableIndexTitle: '索引 ID {indexId}',
    ID: 'ID',
    column: '列',
    includeColumns: '包含的列',
    sort: '顺序',
    delIndexTip: '确定要删除该索引吗？',
    delIndex: '删除索引',
    aggTableIndexTips: '当前无明细索引。请将被使用在明细查询中的列设为明细索引。',
    tableIndex: '明细索引',
    addTableIndex: '添加明细索引',
    cardinality: '基数',
    indexTimeRange: '索引时间范围',
    refuseAddIndexTip: '无法添加实时索引。请先停止实时任务，再清空实时 Segment。',
    refuseRemoveIndexTip: '无法删除实时索引。请先停止实时任务，再清空实时 Segment。',
    refuseEditIndexTip: '无法编辑实时索引。请先停止实时任务，再清空实时 Segment。'
  }
}
