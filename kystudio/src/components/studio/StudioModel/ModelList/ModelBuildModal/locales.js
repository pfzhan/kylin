export default {
  'en': {
    dataRangeValValid: 'Please enter build date range',
    modelBuild: 'Load Data',
    buildRange: 'Build Range',
    startDate: 'Start Date',
    endDate: 'End Date',
    to: 'To',
    loadExistingData: 'Load existing data',
    loadExistingDataDesc: 'Load new records existing from the last load job.',
    customLoadRange: 'Customize Load Range',
    detectAvailableRange: 'Detect available range',
    invaildDate: 'Please enter a valid date',
    overlapsTips: 'The range of load data overlaps with the current segments. Please modify.',
    segmentHoletips: 'Gap(s) exists in the current segment range. The data within the gap can\'t be queried. Do you want to add the following segments?',
    fixSegmentTitle: 'Fix Segment',
    ignore: 'Ignore',
    fixAndBuild: 'Fix and Build',
    chooseBuildType: 'Please select a build type',
    build: 'Load Data',
    buildIndex: 'Build Index',
    incremental: 'Incremental Build',
    fullLoad: 'Full Build',
    complete: 'Build Index',
    incrementalTips: 'The system could incrementally load data based on the selected partition column.',
    fullLoadTips: 'All data under this model would be built.',
    fullLoadBuildTips: 'Please note that all data (in total {storageSize}) would be rebuilt. ',
    completeTips: 'The data would be incrementally built based on the chosen partition column.',
    partitionDateColumn: 'Time Partition Column',
    noPartition: 'No Partition',
    dateFormat: 'Time Format',
    pleaseInputColumn: 'Please select time format',
    detectFormat: 'Detect partition time format',
    showMore: 'View partition settings',
    showLess: 'Hide partition settings',
    pleaseSetDataRange: 'Please select the time range to load data and build index',
    changeBuildTypeTips: 'With partition setting changed, all segments and data would be deleted. The model couldn’t serve queries. Meanwhile, the related ongoing jobs for building index would be discarded. ',
    changeSegmentTip1: 'You have modified the partition column as {tableColumn}, time format {dateType}. After saving, all segments under the model {modelName} will be purged. You need to reload the data, the model cannot serve related queries during data loading. Please confirm whether to submit?',
    changeSegmentTip2: 'You have modified as no partition column. After saving, all segments under the model {modelName} will be purged . The system will automatically rebuild the index and full load the data. The model cannot serve related queries during index building. Please confirm whether to submit?',
    changeSegmentTips: 'With partition setting changed, all segments and data would be deleted. The model couldn\'t serve queries. Meanwhile, the related ongoing jobs for building index would be discarded. <br/>Do you want to continue?',
    unableComplete: 'All indexes of this model are complete and do not need to be completed.',
    invaildDateNoEqual: 'The start time (inclusive) cannot be equal to the end time (not included)',
    changeBuildTypeTipsConfirm: 'You have modified as full load. After saving, all segments under the model {modelName} will be purged . The system will automatically rebuild the index and full load the data. The model cannot serve related queries during index building. Please confirm whether to submit?',
    partitionFirst: 'Please set the time partition column first',
    segmentTips: 'Segment is used to define model\'s data range (served for queries). The data range of segments in total equals the model\'s data range. Queries within the range could be answered by indexes or pushdown engine. Queries out of the range would have no results.',
    addRangeTitle: 'Please select a segment\'s data range',
    willAddSegmentTips: 'A segment for full build would be added and served for queries.',
    saveAndBuild: 'Save and Build Index',
    saveAndAddIndex: 'Save and Add Index',
    onlySaveTip1: 'An empty segment (with no index) would be created after saving. Please note that queries would be answered by the pushdown engine when  they hit empty segments. ',
    onlySaveTip2: 'Do you want to continue?',
    multiPartitionValue: 'Select Subpartition Values ',
    subPartitionAlert: 'Please ensure that the subpartition value is consistent with what it is in the source table (case-sensitive). Otherwise, the built data will be empty. The new values added here would be saved to the subpartition value list of this model.',
    selectInput: 'Select Input',
    batchInput: 'Batch Input',
    pleaseInputOrSearch: 'Please add or search select',
    multiPartitionPlaceholder: 'Please select or import, use comma (,) to separate multiple values',
    multipleBuild: 'Build multiple subpartitions in parallel',
    multipleBuildTip: 'By default, only one build job would be generated for all subpartitions. With this option checked, multiple jobs would be generated and subpartitions would be built in parallel.',
    duplicatePartitionValueTip: 'Some values are not valid, as they are duplicated, or have been built.',
    removeDuplicateValue: 'Clear invalid values',
    multilevelPartition: 'Subpartition Column',
    multilevelPartitionDesc: 'A column from the selected table could be chosen. The models under this project could be partitioned by this column in addition to time partitioning. '
  },
  'zh-cn': {
    dataRangeValValid: '请输入构建日期范围',
    modelBuild: '加载数据',
    buildRange: '构建范围',
    startDate: '起始日期',
    endDate: '截止日期',
    to: '至',
    loadExistingData: '加载已有数据',
    loadExistingDataDesc: '加载从最后一次任务开始之后的最新的数据。',
    customLoadRange: '自定义加载数据范围',
    detectAvailableRange: '获取最新数据范围',
    invaildDate: '请输入合法的时间区间。',
    overlapsTips: '加载的数据范围与当前 Segment 重叠，请检查并修改。',
    segmentHoletips: '当前 Segment 区间存在空洞，此时将无法查询到该段时间的数据，是否需要补充以下 Segment 进行修复？',
    fixSegmentTitle: '修复 Segment',
    ignore: '忽略',
    fixAndBuild: '修复并构建',
    chooseBuildType: '请选择构建方式',
    build: '加载数据',
    buildIndex: '构建索引',
    incremental: '增量构建',
    fullLoad: '全量构建',
    complete: '构建索引',
    incrementalTips: '系统可以根据您选择的分区列，增量加载数据。',
    fullLoadTips: '本次构建将包括模型下所有的数据。',
    fullLoadBuildTips: '本次构建将包括模型下所有的数据，共 {storageSize}。',
    completeTips: '系统将按照选择的分区列进行增量构建。',
    partitionDateColumn: '时间分区列',
    noPartition: '无分区',
    dateFormat: '时间格式',
    pleaseInputColumn: '请选择时间格式',
    detectFormat: '获取分区列时间格式',
    showMore: '显示分区列设置',
    showLess: '隐藏分区列设置',
    pleaseSetDataRange: '请选择构建索引的时间范围',
    changeBuildTypeTips: '修改模型分区设置后，系统将删除所有 Segment 及数据，模型将无法服务于业务查询。同时正在执行的构建任务将被终止。',
    changeSegmentTip1: '您修改分区列为 {tableColumn}，格式为 {dateType}，保存后会导致模型 {modelName} 下的所有 Segments 被清空。您需要重新加载数据，数据加载期间该模型不能服务于相关的查询。请确认是否提交？',
    changeSegmentTip2: '您修改为无分区列，保存后会导致模型 {modelName} 下所有 Segments 被清空。系统将自动重新构建索引并全量加载数据，索引构建期间该模型不能服务于相关的查询。请确认是否提交？',
    changeSegmentTips: '修改模型分区设置后，系统将删除所有 Segment 及数据，模型将无法服务于业务查询。同时正在执行的构建任务将被终止。<br/>是否要继续保存？',
    unableComplete: '该模型所有的索引均完整，无需补全。',
    invaildDateNoEqual: '起始时间（包含）不能等于终止时间（不包含）',
    changeBuildTypeTipsConfirm: '您修改为全量加载，保存后会导致模型 {modelName} 下所有 Segments 被清空。系统将自动重新构建索引并全量加载数据，索引构建期间该模型不能服务于相关的查询。请确认是否提交？',
    partitionFirst: '请您先设置分区列',
    segmentTips: '系统用 Segment 来定义模型服务的数据范围，全部 Segment 的数据范围即模型服务的数据范围。在范围内的查询将通过索引或下压回答，超出范围的查询结果为空。',
    addRangeTitle: '请添加 Segment 的数据范围',
    willAddSegmentTips: '将添加一个全量 Segment 用于服务查询',
    saveAndBuild: '保存并构建索引',
    saveAndAddIndex: '保存并添加索引',
    onlySaveTip1: '保存后将产生一个不包含任何索引数据的空 Segment。如果查询命中空的 Segment, 将会通过下压查询回答。',
    onlySaveTip2: '确定要保存吗？',
    multiPartitionValue: '选择子分区值',
    subPartitionAlert: '请确保子分区值与源表中一致（大小写敏感），否则构建数据将为空。新添加的子分区值将被保存到该模型的子分区值列表中。',
    selectInput: '选择输入',
    batchInput: '批量输入',
    pleaseInputOrSearch: '请输入后回车添加或者搜索选择',
    multiPartitionPlaceholder: '请选择或者输入，多个值请用逗号（,）分隔',
    multipleBuild: '拆分多个任务并发构建',
    multipleBuildTip: '系统默认仅生成一个构建任务。勾选此选项后，将根据所选的子分区生成多个对应的任务，进行并发构建。',
    duplicatePartitionValueTip: '存在无效的输入值，可能因为该值重复或已被构建。',
    removeDuplicateValue: '清空无效的值',
    multilevelPartition: '子分区列',
    multilevelPartitionDesc: '可选择表上的一列作为子分区，对模型进行分区管理。'
  }
}
