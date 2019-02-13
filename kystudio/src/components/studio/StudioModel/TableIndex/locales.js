export default {
  en: {
    noSupportRawTable: 'Only KAP PLUS Provides Raw Table',
    tableIndex: 'Table Index',
    ID: 'ID',
    column: 'Column',
    dataType: 'Data Type',
    tableAlias: 'Table Alias',
    Encoding: 'Encoding',
    Length: 'Length',
    Index: 'Index',
    ConfigRawTable: 'Config Table Index',
    sortBy: 'Sort By',
    shardBy: 'Shard By',
    checkRowkeyInt: 'Integer encoding column length should between 1 and 8.',
    fixedLengthTip:
      'The length parameter of Fixed Length encoding is required.',
    fixedLengthHexTip:
      'The length parameter of Fixed Length Hex encoding is required.',
    fuzzyTip: 'Fuzzy index can be applied to string(varchar) type column only.',
    rawtableSortedWidthDate:
      'The first \'sorted\' column should be a column with encoding \'integer\', \'time\' or \'date\'.',
    rawtableSetSorted:
      'Please set at least one column with Sort By value of \'true\'.',
    sortByNull: 'The \'sorted\' column should not be null',
    tableIndexDetail: 'Table Index Detail',
    today: 'Today',
    thisWeek: 'This Week',
    lastWeek: 'Last Week',
    longAgo: 'Long Ago',
    tableIndexName: 'Table Index Name:',
    tableIndexId: 'Table Index ID:',
    searchTip: 'Search Table Index ID',
    broken: 'Broken',
    available: 'Available',
    empty: 'Emtpy Index',
    manualAdvice: 'User-defined index',
    autoAdvice: 'System-defined index'
  },
  'zh-cn': {
    noSupportRawTable: '只有KAP PLUS 提供Raw Table功能',
    tableIndex: '表明细索引',
    ID: 'ID',
    column: '列',
    dataType: '数据类型',
    tableAlias: '表别名',
    Encoding: '编码',
    Length: '长度',
    Index: '索引',
    ConfigRawTable: '配置Table Index',
    sortBy: 'Sort By',
    shardBy: 'Shard By',
    checkRowkeyInt: '编码为integer的列的长度应该在1至8之间。',
    fixedLengthTip: 'Fixed Length编码时需要长度参数。',
    fixedLengthHexTip: 'Fixed Length Hex编码时需要长度参数。',
    fuzzyTip: '模糊(fuzzy)索引只支持应用于string（varchar）类型数据。',
    rawtableSortedWidthDate:
      '第一个sorted列应为编码为integer、date或time的列。',
    rawtableSetSorted: '至少设置一个列的Sort By值为\'true\'。',
    sortByNull: 'sorted列不应为null',
    tableIndexDetail: '表明细索引详情',
    today: '今日',
    thisWeek: '本周',
    lastWeek: '上周',
    longAgo: '很久以前',
    tableIndexName: '表明细索引名：',
    tableIndexId: '表明细索引ID：',
    searchTip: '搜索明细索引ID',
    broken: '破损',
    available: '可用',
    empty: '空索引',
    manualAdvice: '用户定义的索引',
    autoAdvice: '系统推荐的索引'
  }
}
