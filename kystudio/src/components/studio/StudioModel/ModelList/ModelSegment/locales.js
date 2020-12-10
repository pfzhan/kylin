export default {
  'en': {
    autoMerge: 'Auto-Merge',
    retension: 'Retension',
    volatile: 'Volatile',
    textInput: 'Text Input',
    segmentPeriod: 'Segment Period:',
    segmentPeriod2: 'Segment Period',
    chooseStartDate: 'Choose Start Time',
    chooseEndDate: 'Choose End Time',
    primaryPartition: 'Primary Partition',
    pleaseChoose: 'Please Choose',
    merge: 'Merge',
    segmentSetting: 'Segment Setting',
    selectContinueSegments: 'Please select continuous segments',
    pleaseSelectSegments: 'Please select at least one segment',
    confirmDeleteSegments: 'Please confirm whether to discard below segments? Please note that this operation cannot be undone.',
    confirmMergeSegments: 'Do you really need to merge {count} segments?',
    pleaseSelectStartOrEndSegments: 'Please select first or last continuous segments.',
    segmentWarning: 'Please confirm whether to delete below segments, which will cause the model {modelName} to be warning status and the query results will be empty when querying those data ranges. Please note that this operation cannot be undone.',
    storageSize: 'Storage Size',
    segmentDetail: 'Segment Details',
    segmentID: 'Segment ID:',
    segmentName: 'Segment Name:',
    segmentPath: 'Segment Path:',
    fileNumber: 'File Number:',
    storageSize1: 'Storage Size:',
    startTime: 'Start Time:',
    endTime: 'End Time:',
    tableIndexSegmentPath: 'Table Index Segment Path:',
    tableIndexFileNumber: 'Table Index File Number:',
    tableIndexStorageSize: 'Table Index Storage Size:',
    fullLoad: 'Full Load',
    confirmRefreshSegments: 'Are you sure to refresh the following segment(s)? The data would be refreshed according to the original indexes in the segment.',
    confirmRefreshSegments2: 'Refresh segment would refresh data and build index. The following segments have incomplete indexes. Please select how to build. The other segments would build their current indexes.',
    segmentIsEmpty: 'There are no segments in this model, so it can\'t be purged.',
    refreshSegmentsTitle: 'Refresh Segment',
    deleteSegmentTip: 'Delete Segment',
    mergeSegmentTip: 'Merge Segment',
    showDetail: 'Show Detail',
    ONLINE: 'Queries within the range could be answered  by indexes or pushdown engine already.',
    LOADING: 'The data in the segment is loading.',
    REFRESHING: 'A new segment is automatically generated when you refresh the specified segment. This new segment is marked as REFRESHING. When the refresh is complete the old segment will be automatically deleted.',
    MERGING: 'A new segment is automatically generated when you merge the specified segments. This new segment is marked as MERGING. When the merge is complete the old segment will be automatically deleted.',
    LOCKED: 'The segment is locked and is being referenced by a refreshed or merged segment. Locked segment cannot be refreshed and deleted.',
    WARNING: 'Data in the source table for this segment was changed. The query results might be inaccurate. Please try refreshing.',
    fix: 'Fix',
    fixTips: 'Automatic fix for discontinuous segments',
    addSegment: 'Add Segment',
    modifyTime: 'Last Updated Time',
    sourceRecords: 'Source Records',
    segmentList: 'Segment List',
    segmentSubTitle: 'Add segments to define the model\'s data range for serving queries.',
    currentIndexes: 'Current Index Amount',
    buildCurrentIndexes: 'Build current indexes',
    buildAllIndexes: 'Build all indexes',
    buildAllIndexesTips: 'To ONLY build all indexes to the selected segments, it\'s recommended to build index on \'Index Overview\' tab. It would save cost for refreshing data.',
    viewSubParValuesBtn: 'View model subpartition Values',
    subPratitionAmount: 'Subpartition Amount',
    subPratitionAmountTip: 'Amount of the built ones / Total amount',
    subParValuesTitle: 'Subpartition',
    buildSubSegment: 'Build Subpartition',
    searchPlaceholder: 'Search by subpartition value',
    buildSubParDesc: 'The indexes in this subpartition would be the same as the current segment ',
    multiPartitionPlaceholder: 'Use comma (,) to separate multiple values',
    build: 'Build',
    selectSubPartitionValues: 'Select Subpartition Values',
    multipleBuild: 'Build multiple subpartitions in parallel',
    multipleBuildTip: 'By default, only one build job would be generated for all subpartitions. With this option checked, multiple jobs would be generated and subpartitions would be built in parallel.',
    refreshSubSegmentTitle: 'Refresh Subpartition',
    refreshSubSegmentTip: 'Are you sure you want to refresh the selected {subSegsLength} subpartition(s)? The data would be refreshed according to the original indexes in the segment {indexes}.',
    deleteSubSegmentTitle: 'Delete Subpartition',
    deleteSubSegmentTip: 'The deleted subpartition(s) would be gone forever. Are you sure you want to delete the selected {subSegsLength} subpartition(s)?',
    mergeSegmentsTitle: 'Merge Segment',
    mergeSegmentDesc: 'The segments could be merged if the following conditions are met.',
    mergeNotice1: 'Have same index amount',
    mergeNotice2: 'Have same subpartition amount',
    mergeNotice3: 'Be continuous in time (don\'t exist holes)',
    afterMergeSegment: 'The segments would be as follow after being merged:',
    partitionONLINE: 'This subpartition has been built. It could serve queries.',
    partitionLOADING: 'This subpartition is being built now. It could serve queries until the building job is complete.',
    partitionREFRESHING: 'This subpartition is being refreshed now. The original data could still serve queries until the refresh job is complete.',
    duplicatePartitionValueTip: 'Some values are duplicated.',
    removeDuplicateValue: 'Clear invalid values',
    disabledSubPartitionEnter: 'The segment is in {status} status. Subpartitions can’t be operated at the moment.',
    noIndexTipByBuild: 'You should add indexes first before building.'
  },
  'zh-cn': {
    autoMerge: 'Auto-Merge',
    retension: 'Retension',
    volatile: 'Volatile',
    textInput: 'Text Input',
    segmentPeriod: 'Segment 范围：',
    segmentPeriod2: 'Segment 范围',
    chooseStartDate: '开始时间',
    chooseEndDate: '结束时间',
    primaryPartition: '一级分区',
    pleaseChoose: '请选择',
    merge: '合并',
    segmentSetting: 'Segment 管理参数',
    selectContinueSegments: '请选择时间相邻的 segment',
    pleaseSelectSegments: '至少选择一个 segment',
    confirmDeleteSegments: '请确认是否删除以下 Segment(s)？请注意：删除操作无法撤销。',
    confirmMergeSegments: '您确认要合并 {count} 个 segments？',
    pleaseSelectStartOrEndSegments: '请选择第一个开始或最后一个结束的连续 segments。',
    segmentWarning: '请确认是否删除以下 Segment(s)，删除后将导致模型 {modelName} 处于警告状态，查询对应数据范围时结果将为空。请注意：删除操作无法撤销。',
    storageSize: '存储大小',
    segmentDetail: 'Segment 详情',
    segmentID: 'Segment ID：',
    segmentName: 'Segment 名称：',
    segmentPath: 'Segment 路径：',
    fileNumber: '索引文件数：',
    storageSize1: '存储空间：',
    startTime: '开始时间：',
    endTime: '结束时间：',
    tableIndexSegmentPath: '索引 Segment 路径：',
    tableIndexFileNumber: '索引文件数：',
    tableIndexStorageSize: '索引存储空间：',
    fullLoad: '全量加载',
    confirmRefreshSegments: '确定要刷新以下 Segment 吗？数据会按 Segment 中原有索引数进行刷新。',
    confirmRefreshSegments2: '刷新 Segment 将刷新数据并构建索引。以下 Segment 未包含全部的索引，请选择以何种方式构建。其余 Segment 会按当前索引进行构建并刷新数据。',
    segmentIsEmpty: '模型中没有 segments，无法被清空。',
    refreshSegmentsTitle: '刷新 Segment',
    deleteSegmentTip: '删除 Segment',
    mergeSegmentTip: '合并 Segment',
    showDetail: '查看详情',
    ONLINE: '该 Segment 范围内的查询已经可以通过索引或者查询下压回答。',
    LOADING: '正在加载该 Segment 对应的数据。',
    REFRESHING: '刷新指定 Segment 时自动生成新的 Segment，这个新的 Segment 处于刷新中。当刷新完毕，将自动删除旧的 Segment。',
    MERGING: '合并指定 Segment 时自动生成新的 Segment，这个新的 Segment 处于合并中。当合并完毕，将自动删除旧的 Segment。',
    LOCKED: 'Segment 处于锁定状态下，正在被刷新或被合并的 Segment 引用。当前状态无法进行刷新和删除操作。',
    WARNING: '当前 Segment 中源表发生变化，可能会影响查询结果的准确性，建议刷新。',
    fix: '修复',
    fixTips: '自动补全不连续的 Segment',
    addSegment: '添加 Segment',
    modifyTime: '最后更新时间',
    sourceRecords: '行数',
    segmentList: 'Segment 列表',
    segmentSubTitle: '添加 Segment 来为模型定义可服务查询的数据范围',
    currentIndexes: '当前索引数',
    buildCurrentIndexes: '构建当前索引',
    buildAllIndexes: '构建至全部索引',
    buildAllIndexesTips: '若仅需将 Segment 构建至全部索引，建议到索引总览页面进行操作。仅构建索引将不会消耗额外资源刷新数据。',
    viewSubParValuesBtn: '查看模型子分区值',
    subPratitionAmount: '子分区数',
    subPratitionAmountTip: '已构建子分区数/子分区总数',
    subParValuesTitle: '子分区',
    buildSubSegment: '构建子分区',
    searchPlaceholder: '搜索子分区值',
    buildSubParDesc: '本次构建子分区的索引与当前时间范围 (Segment) 包含的索引数一致',
    multiPartitionPlaceholder: '多个值请用逗号（,）进行分隔',
    build: '构建',
    selectSubPartitionValues: '选择子分区列值',
    multipleBuild: '拆分多个任务并发构建',
    multipleBuildTip: '系统默认仅生成一个构建任务。勾选此选项后，将根据所选的子分区生成多个对应的任务，进行并发构建。',
    refreshSubSegmentTitle: '刷新子分区',
    refreshSubSegmentTip: '确定要刷新选中的 {subSegsLength} 个子分区吗？数据会按 Segment 中原有索引数 {indexes} 进行刷新。',
    deleteSubSegmentTitle: '删除子分区',
    deleteSubSegmentTip: '子分区一旦删除不可撤销。确定要删除所选的 {subSegsLength} 个子分区吗？',
    mergeSegmentsTitle: '合并 Segment',
    mergeSegmentDesc: '合并的 Segment 需要满足以下条件，',
    mergeNotice1: '包含的索引数一致',
    mergeNotice2: '包含的子分区值一致',
    mergeNotice3: '在时间上是连续的（不存在空洞）',
    afterMergeSegment: '已选择的 Segment 合并后如下所示：',
    partitionONLINE: '该子分区已构建完成，可服务于查询。',
    partitionLOADING: '该子分区正在构建中，构建完成后可服务于查询。',
    partitionREFRESHING: '该子分区正在刷新中，刷新完成前原有的数据仍可服务于查询。',
    duplicatePartitionValueTip: '输入值不可重复。',
    removeDuplicateValue: '清空无效的值',
    disabledSubPartitionEnter: '所属 Segment 处于 {status} 状态中，不可操作子分区',
    noIndexTipByBuild: '您需要先添加索引，才可以进行构建。'
  }
}
