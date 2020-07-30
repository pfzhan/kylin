export default {
  'en': {
    autoMerge: 'Auto-Merge',
    retension: 'Retension',
    volatile: 'Volatile',
    textInput: 'Text Input',
    segmentPeriod: 'Segment Period:',
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
    confirmRefreshSegments: 'Do you really need to refresh {count} segments?',
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
    modifyTime: 'Last Modified Time',
    sourceRecords: 'Source Records',
    segmentList: 'Segment List',
    segmentSubTitle: 'Add segments to define the model’s data range for serving queries.'
  },
  'zh-cn': {
    autoMerge: 'Auto-Merge',
    retension: 'Retension',
    volatile: 'Volatile',
    textInput: 'Text Input',
    segmentPeriod: 'Segment 范围：',
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
    confirmRefreshSegments: '您确认要刷新 {count} 个 segments?',
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
    modifyTime: '最后修改时间',
    sourceRecords: '行数',
    segmentList: 'Segment 列表',
    segmentSubTitle: '添加 Segment 来为模型定义可服务查询的数据范围'
  }
}
