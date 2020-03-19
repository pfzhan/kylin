export default {
  'en': {
    aggregateGroup: 'Aggregate Index',
    aggregateAmount: 'Index Amount:',
    aggregateIndexTree: 'Index Treemap',
    aggregateDetail: 'Index List',
    content: 'Content',
    order: 'Order',
    storage: 'Data Size',
    modifiedTime: 'Last Modify Time',
    queryCount: 'Usage',
    usageTip: 'Times of the index used by queries',
    dataRange: 'Data Range',
    searchAggregateID: 'Search index content or ID',
    to: ' to ',
    id: 'Index ID',
    emptyAggregate: 'Empty Index:',
    brokenAggregate: 'Broken Aggregate:',
    buildIndex: 'Build Index',
    aggIndexAdvancedTitle: 'Advanced Setting',
    source: 'Source',
    MANUAL_AGG: 'Custom(Aggregate Group) ',
    AUTO_AGG: 'Recommended(Aggregate Group)',
    MANUAL_TABLE: 'Custom(Table Index)',
    AUTO_TABLE: 'Recommended(Table Index)',
    viewDetail: 'View Index Details',
    tableIndex: 'Table Index',
    delIndex: 'Delete Index',
    delIndexTip: 'Are you sure to delete this index?',
    editIndex: 'Edit',
    ID: 'ID',
    column: 'Column',
    sort: 'Order',
    aggDetailTitle: 'Aggregate Detail',
    tabelDetailTitle: 'Table Index Detail',
    noIndexTips: 'You should add indexes first before building.',
    treemapTips: 'You can intuitively see the data size and usage of different indexes through the index treemap. The larger area represents the larger data size, and the darker color represents the higher usage of indexes. Different types of indexes are displayed in multiple blocks.',
    EMPTY: 'NO BUILD',
    AVAILABLE: 'ONLINE',
    TO_BE_DELETED: 'LOCKED',
    BUILDING: 'BUILDING',
    clearAll: 'Clear All',
    bulidTips: 'Are you sure to build all indexes under the model {modelName}?',
    segmentHoletips: 'There exists a hole in the segment range, and the model will not be able to server queries. Please confirm whether to add the following segments to fix.',
    fixSegmentTitle: 'Fix Segment',
    ignore: 'Ignore',
    fixAndBuild: 'Fix and Build'
  },
  'zh-cn': {
    aggregateGroup: '聚合索引',
    aggregateAmount: '索引总数：',
    aggregateIndexTree: '索引展示图 ',
    aggregateDetail: '索引列表',
    content: '内容',
    order: '顺序',
    storage: '数据大小',
    modifiedTime: '上次更新时间',
    queryCount: '使用次数',
    usageTip: '查询使用该索引的次数',
    dataRange: '数据范围',
    searchAggregateID: '搜索索引内容或 ID',
    to: ' 至 ',
    id: 'Index ID',
    emptyAggregate: '空的索引：',
    brokenAggregate: '破损聚合索引：',
    buildIndex: '构建索引',
    aggIndexAdvancedTitle: '高级设置',
    source: '来源',
    MANUAL_AGG: '自定义聚合索引',
    AUTO_AGG: '系统推荐聚合索引',
    MANUAL_TABLE: '自定义明细索引',
    AUTO_TABLE: '系统推荐明细索引',
    viewDetail: '查看索引详情',
    tableIndex: '明细索引',
    delIndex: '删除索引',
    delIndexTip: '您确认要删除该索引吗？',
    editIndex: '编辑索引',
    ID: 'ID',
    column: '列',
    sort: '顺序',
    aggDetailTitle: '聚合索引详情',
    tabelDetailTitle: '明细索引详情',
    noIndexTips: '您需要先添加索引，才可以进行构建。',
    treemapTips: '您可以通过索引展示图直观地看到不同索引的数据大小和使用次数，面积越大代表数据大小越大，颜色越深代表使用次数越高。不同类型的索引将分为多块显示。',
    EMPTY: '未构建',
    AVAILABLE: '在线',
    TO_BE_DELETED: '锁定',
    BUILDING: '构建中',
    clearAll: '清除所有',
    bulidTips: '你确认要构建模型 {modelName} 下的所有索引吗？',
    segmentHoletips: '当前 Segment 区间存在空洞，此时将无法服务于查询，是否需要补充以下 Segment 进行修复？',
    fixSegmentTitle: '修复 Segment',
    ignore: '忽略',
    fixAndBuild: '修复并构建'
  }
}
