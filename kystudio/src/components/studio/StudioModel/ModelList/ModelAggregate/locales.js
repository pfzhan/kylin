export default {
  'en': {
    aggregateGroup: 'Aggregate Group',
    aggregateAmount: 'Index Amount:',
    aggregateIndexTree: 'Index Treemap',
    aggregateDetail: 'Index List',
    content: 'Content',
    order: 'Order',
    storage: 'Storage',
    modifiedTime: 'Last Updated Time',
    queryCount: 'Usage',
    usageTip: 'Times of the index used by queries',
    dataRange: 'Model Data Range',
    dataRangeTips: 'The data range of this model to serve queries',
    noDataRange: 'No data range',
    viewIncomplete: 'View ranges with incomplete indexes',
    searchAggregateID: 'Search index content or ID',
    to: ' to ',
    id: 'Index ID',
    emptyAggregate: 'Empty Index:',
    brokenAggregate: 'Broken Aggregate:',
    buildIndex: 'Build Index',
    aggIndexAdvancedTitle: 'Advanced Setting',
    source: 'Source',
    CUSTOM_AGG_INDEX: 'Custom Aggregate Group',
    RECOMMENDED_AGG_INDEX: 'Recommended Aggregate Index',
    CUSTOM_TABLE_INDEX: 'Custom Table Index',
    RECOMMENDED_TABLE_INDEX: 'Recommended Table Index',
    BASE_AGG_INDEX: 'Base Aggregate Index',
    BASE_TABLE_INDEX: 'Base Table Index',
    customAggIndex: 'Custom Aggregate Group: ',
    customTableIndex: 'Custom Table Index: ',
    indexContentTips: '{dimensionNum} dimension(s), {measureNum} measure(s)',
    tableIndexContentTip: '{num} column(s)',
    viewDetail: 'View Index Details',
    tableIndex: 'Table Index',
    delIndex: 'Delete Index',
    delIndexTip: 'Are you sure you want to delete this index?',
    delIndexesTips: 'Do you want to delete the selected {indexNum} index(es) from all segments? The query performance might be affected once indexes are deleted.',
    editIndex: 'Edit',
    ID: 'ID',
    column: 'Column',
    sort: 'Order',
    update: 'Update',
    updateBaseIndexTitle: 'Update Base Indexes',
    aggDetailTitle: 'Aggregate Detail',
    tabelDetailTitle: 'Table Index Detail',
    noIndexTips: 'You should add indexes first before building.',
    treemapTips: 'The larger area represents the larger data size. The darker color represents higher usage. Different types of indexes are displayed in multiple blocks.',
    NO_BUILD: 'NO BUILD',
    ONLINE: 'ONLINE',
    LOCKED: 'LOCKED',
    BUILDING: 'BUILDING',
    clearAll: 'Clear All',
    bulidTips: 'Are you sure you want to build all indexes under the model "{modelName}"?',
    segmentHoletips: 'This model\'s segment range has gaps in between.  Empty results might be returned when querying those ranges. Please confirm whether to add the following segments to fix.',
    fixSegmentTitle: 'Fix Segment',
    ignore: 'Ignore',
    fixAndBuild: 'Fix and Build',
    index: 'Index',
    indexListBtn: 'Index List',
    recommendationsBtn: 'Recommendations',
    viewIncompleteTitle: 'Data Ranges with Incomplete Indexes',
    incompleteSubTitle: 'Some data ranges of this model have incomplete indexes. To improve query performance, it\'s recommended to select all and build index. ',
    batchBuildSubTitle: 'Please choose which data ranges you\'d like to build with the selected {number} index(es).',
    subTitle: 'This index hasn\'t been fully built to the following data ranges. To improve query performance, it\'s recommended to select all and build index. ',
    deleteIndex: 'Delete Index',
    deleteTips: 'Do you want to delete the selected {number} index(es) from the following segment(s)? The query performance might be affected once indexes are deleted.',
    deletePart: 'Delete from Selected Segment',
    indexSubTitle: 'The data range of this model to serve queries. You could selectively build indexes to desired data ranges based on query requirements.',
    indexesByQueryHistoryTip: 'View the anwered index(es) only',
    indexContent: 'Content',
    indexesContent: 'Index Content',
    viewIndexDetails: 'More details',
    viewAll: 'View all',
    cardinality: 'Cardinality',
    cardinalityColumnTips: 'Total amount of unique data in this column. Could be gathered from sampling.',
    baseIndex: 'Base Index',
    unCreateBaseIndexTip: 'Base Indexes have already been added',
    buildBaseIndexTip: 'Successfully added {baseIndexNum} base index(es). ',
    updateBaseIndexTips: 'A base index would be added. The previous base index might be in "locked" state while still could answer queries. Are you sure you wan to update？'
  },
  'zh-cn': {
    aggregateGroup: '聚合组',
    aggregateAmount: '索引总数：',
    aggregateIndexTree: '索引展示图 ',
    aggregateDetail: '索引列表',
    content: '内容',
    order: '顺序',
    storage: '存储',
    modifiedTime: '最后更新时间',
    queryCount: '使用次数',
    usageTip: '查询使用该索引的次数',
    dataRange: '模型数据范围',
    dataRangeTips: '模型可服务查询的时间范围',
    noDataRange: '无数据范围',
    viewIncomplete: '查看索引不完整的范围',
    searchAggregateID: '搜索索引内容或 ID',
    to: ' 至 ',
    id: 'Index ID',
    emptyAggregate: '空的索引：',
    brokenAggregate: '破损聚合索引：',
    buildIndex: '构建索引',
    aggIndexAdvancedTitle: '高级设置',
    source: '来源',
    CUSTOM_AGG_INDEX: '自定义聚合索引',
    RECOMMENDED_AGG_INDEX: '系统推荐聚合索引',
    CUSTOM_TABLE_INDEX: '自定义明细索引',
    RECOMMENDED_TABLE_INDEX: '系统推荐明细索引',
    BASE_TABLE_INDEX: '基础明细索引',
    BASE_AGG_INDEX: '基础聚合索引',
    customAggIndex: '自定义聚合索引：',
    customTableIndex: '自定义明细索引：',
    indexContentTips: '{dimensionNum} 个维度，{measureNum} 个度量',
    tableIndexContentTip: '{num} 个列',
    viewDetail: '查看索引详情',
    tableIndex: '明细索引',
    delIndex: '删除索引',
    delIndexTip: '确定要删除该索引吗？',
    delIndexesTips: '确定从所有 Segment 中删除 {indexNum} 个已选择的索引吗？删除后可能会影响相关索引的查询效率。',
    editIndex: '编辑索引',
    ID: 'ID',
    column: '列',
    sort: '顺序',
    update: '更新',
    updateBaseIndexTitle: '更新基础索引',
    aggDetailTitle: '聚合索引详情',
    tabelDetailTitle: '明细索引详情',
    noIndexTips: '您需要先添加索引，才可以进行构建。',
    treemapTips: '面积越大代表数据越大，颜色越深代表使用次数越多。不同类型的索引将分为多块显示。',
    NO_BUILD: '未构建',
    ONLINE: '在线',
    LOCKED: '锁定',
    BUILDING: '构建中',
    clearAll: '清除所有',
    bulidTips: '确认要构建模型 ”{modelName}“ 下的所有索引吗？',
    segmentHoletips: '当前 Segment 区间存在空洞，此时将无法查询到该段时间的数据，是否需要补充以下 Segment 进行修复？',
    fixSegmentTitle: '修复 Segment',
    ignore: '忽略',
    fixAndBuild: '修复并构建',
    index: '索引',
    indexListBtn: '索引列表',
    recommendationsBtn: '优化建议',
    viewIncompleteTitle: '索引不完整的数据范围',
    incompleteSubTitle: '模型中有以下数据范围索引不完整。为了提高查询效率，建议您将全部索引构建至以下数据范围。',
    batchBuildSubTitle: '请为选中的 {number} 个索引选择需要构建至的数据范围。',
    subTitle: '该索引未构建至以下数据范围。为了提高查询效率，建议您将该索引构建至以下数据范围。',
    deleteIndex: '删除索引',
    deleteTips: '确定从以下的 Segment 中删除 {number} 个已选择的索引吗？删除后可能会影响相关索引的查询效率。',
    deletePart: '从部分 Segment 中删除',
    indexSubTitle: '模型可服务查询的数据范围。您可视需求将所需的索引构建至相应的数据范围。',
    indexesByQueryHistoryTip: '仅查看回答查询的索引',
    indexContent: '内容',
    indexesContent: '索引内容',
    viewIndexDetails: '更多详情',
    viewAll: '查看全部',
    cardinality: '基数',
    cardinalityColumnTips: '该列不重复的数据量。该数据可通过采样获得。',
    baseIndex: '基础索引',
    unCreateBaseIndexTip: '基础索引已添加',
    buildBaseIndexTip: '成功添加 {baseIndexNum} 个基础索引。',
    updateBaseIndexTips: '将新建 1 个基础索引。原基础索引可能处于“锁定”状态，但仍可服务查询。确定要更新吗？'
  }
}
