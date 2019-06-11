export default {
  'en': {
    incrementalLoading: 'Incremental Load',
    refreshData: 'Refresh Data',
    refreshRange: 'Refresh Range',
    refreshRangeDesc: 'The refreshing range should not exceed the loaded data range. If the range cross over several segments, the system will notify you of the refreshing workload.',
    dataMerge: 'Merge Data',
    loadingRange: 'Loading Range',
    dataRange: 'Data Range',
    partitionColumn: 'Partition Column',
    refreshTitle: 'Refresh Range',
    refreshContent1: 'You\'re going to refresh the table {tableName}. The size of the impacted data would be {storageSize}.',
    refreshContent2: 'Do you need to submit the refresh job?',
    HALF_MINUTE: 'Half minute',
    FIVE_MINUTE: 'Five minute',
    TEN_MINUTE: 'Ten minute',
    HALF_HOUR: 'Half hour',
    HOUR: 'One hour',
    SIX_HOUR: 'Six hour',
    HALF_DAY: 'Half day',
    DAY: 'One day',
    WEEK: 'One week',
    MONTH: 'One month',
    QUARTER: 'One quarter',
    YEAR: 'One year',
    isMergeable: 'Do you want to enable auto-merge?',
    mergePreference: 'Merge Preference',
    autoMerge: 'Auto-Merge',
    volatile: 'Volatile Range',
    minute: 'Minute',
    hour: 'Hour',
    day: 'Day',
    month: 'Month',
    year: 'Year',
    pushdownRange: 'Pushdown Range',
    pushdownDesc: 'To keep query result consistent, the source data will be accessible to push-down query only if they are within the data range.',
    isPushdown: 'Sure, pushdown query should be within the data range.',
    notPushdown: 'No, I want to query all source data via pushdown engine.',
    invaildDate: 'Please input vaild date.',
    minValueInvaild: 'Start data range has the running jobs, please select the start time before.',
    ON: 'ON',
    OFF: 'OFF',
    refreshRangeTip: 'Reload updated records from the source table.',
    autoMergeTip: 'The system can auto-merge segment fragments over different merge threshold. Auto-merge, like defragmentation, will optimize storage to enhance query performance.',
    volatileTip: 'The latest data may change later, the data in the volatile range will not be merged.',
    selectAll: 'Select All',
    partitionFormat: 'Partition Format',
    loadData: 'Load Data',
    loadExistingData: 'Load existing data',
    loadExistingDataDesc: 'Load new records existing from the last load job.',
    customLoadRange: 'Custom Load Range',
    detectAvailableRange: 'Detect available range'
  },
  'zh-cn': {
    incrementalLoading: '增量加载',
    refreshData: '刷新数据',
    refreshRange: '刷新范围',
    refreshRangeDesc: '刷新数据范围不能大于已加载的数据范围。如果刷新的范围包含多个segments，系统将会提示工作量。',
    dataMerge: '合并数据',
    loadingRange: '加载范围',
    dataRange: 'Data Range',
    partitionColumn: '分区列',
    refreshTitle: '刷新范围',
    refreshContent1: '刷新表 {tableName} 时，会刷新本表已经加载的 {storageSize} 数据。',
    refreshContent2: '您是否提交刷新任务？',
    HALF_MINUTE: '30 秒',
    FIVE_MINUTE: '5 分钟',
    TEN_MINUTE: '10 分钟',
    HALF_HOUR: '30 分钟',
    HOUR: '一小时',
    SIX_HOUR: '6 小时',
    HALF_DAY: '12 小时',
    DAY: '一天',
    WEEK: '一周',
    MONTH: '一月',
    QUARTER: '三个月',
    YEAR: '一年',
    isMergeable: '是否开启自动合并？',
    mergePreference: '合并偏好',
    autoMerge: '自动合并',
    volatile: '变动范围',
    minute: '分钟',
    hour: '小时',
    day: '天',
    month: '月',
    year: '年',
    pushdownRange: '下压查询的范围',
    pushdownDesc: '为了确保查询结果的一致性，系统默认查询下压将遵循源表上的数据范围，超出范围的源数据无法被查到。',
    isPushdown: '同意，查询下压范围应该在数据范围之内。',
    notPushdown: '不同意，我希望可以通过下压查询查到所有源数据。',
    invaildDate: '请输入合法的时间区间。',
    minValueInvaild: '开始时间包含正在运行的数据加载任务，请向前选择开始时间。',
    ON: '开启',
    OFF: '关闭',
    refreshRangeTip: '重新从源表上加载一段更新后的交易记录。',
    autoMergeTip: '根据不同层级的时间周期，系统可以自动合并 segment 碎片。合并 segment 就像碎片整理，可以优化查询提升查询性能。',
    volatileTip: '处于变动中的一段最新的数据，处于变动范围的数据不会被合并。',
    selectAll: '选择所有数据区间',
    partitionFormat: '时间格式',
    loadData: '加载数据',
    loadExistingData: '加载已有数据',
    loadExistingDataDesc: '加载从最后一次任务开始之后的最新数据。',
    customLoadRange: '自定义数据范围',
    detectAvailableRange: '获取最新数据范围'
  }
}
