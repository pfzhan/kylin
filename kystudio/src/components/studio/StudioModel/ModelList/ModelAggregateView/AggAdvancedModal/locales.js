export default {
  'en': {
    select: 'Selected',
    sort: 'Order',
    aggIndexAdvancedTitle: 'Advanced Setting',
    advancedTips: 'Data could be distributed into multiple shards according to the ShardBy column to improve query efficiency. It is recommended to set only one ShardBy column. Please set the filter dimension or grouping dimension frequently used in the query as the ShardBy column according to the cardinality from high to low.',
    dimension: 'Dimension',
    filter: 'Search Dimension'
  },
  'zh-cn': {
    select: '选中',
    sort: '顺序',
    aggIndexAdvancedTitle: '高级设置',
    advancedTips: '按照 ShardBy 列分片存储数据以提高查询效率。建议仅设置一个 ShardBy 列。请将查询中常用的过滤维度或分组维度按照基数从大到小设置为 ShardBy 列。',
    dimension: '维度',
    filter: '搜索维度'
  }
}
