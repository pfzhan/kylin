export default {
  'en': {
    capbility: 'Favorite Rate',
    dataLoadTime: 'Last Updated Time: ',
    status: 'Status',
    streamingTips: '实时模型，部分功能不可用',
    modelPartitionSet: 'Model Partition',
    enableModelTip: 'Are you sure you want to online the model {modelName}?',
    enableModelTitle: 'Online Model',
    noModel: 'You can click below button to add a model',
    datacheck: 'Data Check',
    favorite: 'Favorite',
    importMdx: 'Import MDX',
    exportTds: 'Export TDS',
    exportMdx: 'Export MDX',
    rename: 'Rename',
    delete: 'Delete',
    purge: 'Purge',
    disable: 'Disable',
    enable: 'Enable',
    recommendationsTiTle: 'Recommendations',
    inputModelName: 'Please enter new model name',
    inputCloneName: 'Please enter clone model name',
    segment: 'Segment',
    aggregateGroup: 'Aggregate Group',
    tableIndex: 'Table Index',
    indexOverview: 'Index Overview',
    onLine: 'Online',
    offLine: 'Offline',
    build: 'Build Index',
    buildTitle: 'Build',
    storage: 'Storage',
    usage: 'Usage',
    rowCount: 'Rows',
    secStorage: 'Tiered Storage',
    fullLoadDataTitle: 'Load Data',
    fullLoadDataContent1: 'The model {modelName} has no partition.',
    fullLoadDataContent2: '{storageSize} storage would be required as all data would be reloaded.',
    fullLoadDataContent3: 'Are you sure you want to reload the data?',
    fullScreen: 'Full Screen',
    exitFullScreen: 'Exit Full Screen',
    usageTip: 'Times of the {mode} used by queries',
    model: 'model',
    description: 'Description',
    indexGroup: 'index group',
    expansionRate: 'Expansion Rate',
    expansionRateTip: 'Expansion Rate = Storage Size / Source Table Size<br/>Note: The expansion rate won\'t show if the storage size is less than 1 GB.',
    tentative: 'Tentative',
    recommendations: 'Recommendation',
    recommendationsTip: 'By analyzing the query history and model usage, the system will provide some recommendations.',
    clearAll: 'Clear All',
    ALL: 'All',
    ONLINE: 'ONLINE',
    OFFLINE: 'OFFLINE',
    BROKEN: 'BROKEN',
    status_c: 'Status: ',
    modelStatus_c: 'Model Status:',
    modelType_c: 'Model Attributes: ',
    SECOND_STORAGE: 'Model with tiered storage',
    sec_storage_model: 'Model with secondary storage',
    others: 'Others',
    reset: 'Reset',
    lastModifyTime_c: 'Last Updated Time: ',
    lastBuildTime: 'Last Build Time: ',
    allTimeRange: 'All Time Range',
    filterButton: 'Filter',
    aggIndexCount: 'Index Amount',
    recommendations_c: 'Recommendation: ',
    clickToView: 'Review',
    filterModelOrOwner: 'Search model name',
    modelSegmentHoleTips: 'This model\'s segment range has gaps in between. Empty results might be returned when querying those ranges. ',
    autoFix: 'automatic fix',
    ID: 'ID',
    column: 'Column',
    sort: 'Order',
    buildTips: 'The indexes in this model have not been built and are not ready for serving queries. Please build indexes to optimize query performance.',
    iKnow: 'Got it',
    exportMetadata: 'Export Metadata',
    noModelsExport: 'The model\'s metadata can\'t be exported as there are no models yet.',
    exportMetadatas: 'Export Model',
    exportMetadataSuccess: 'A model metadata package is being generated. Download will start automatically once ready.',
    exportMetadataFailed: 'Can\'t export models at the moment. Please try again.',
    bokenModelExportMetadatasTip: 'Can\'t export model file at the moment as the model is BROKEN',
    importModels: 'Import Model',
    emptyIndexTips: 'This model has unbuilt indexes. Please click the build index button to build the indexes.',
    guideToAcceptRecom: 'You may click on the recommendations besides the model name to view how to optimize.',
    overview: 'Overview',
    changeModelOwner: 'Change Owner',
    change: 'Change',
    modelName: 'Model',
    modelType: 'Types',
    STREAMING: 'Streaming Model',
    BATCH: 'Batch Model',
    HYBRID: 'Hybrid Model',
    changeTo: 'Change Owner To',
    pleaseChangeOwner: 'Please change model owner',
    changeDesc: 'You can change the owner of the model to a system admin, a user in the project ADMIN role, or a user in the project management role.',
    buildIndex: 'Build Index',
    batchBuildSubTitle: 'Please choose which data ranges you\'d like to build with the added indexes.',
    modelMetadataChangedTips: 'Data in the source table was changed. The query results might be inaccurate. ',
    seeDetail: 'View Details',
    // modelMetadataChangedDesc: 'Source table in the following segment(s) might have been changed. The data might be inconsistent after being built. Please check with your system admin.<br/>You may try refreshing these segments to ensure the data consistency.',
    noSegmentOnlineTip: 'This model can\'t go online as it doesn\'t have segments. Models with no segment couldn\'t serve queries. Please add a segment.',
    refrashWarningSegment: 'Only ONLINE segments could be refreshed',
    closeSCD2ModalOnlineTip: 'This model can\'t go online as it includes non-equal join conditions(≥, <). Please delete those join conditions, or turn on `Support History table` in project settings.',
    SCD2ModalOfflineTip: 'This model includes non-equal join conditions (≥, <), which are not supported at the moment. Please delete those join conditions, or turn on `Support History table` in project settings.',
    storageTip: 'Calculates the amount of data built in this model',
    subPartitionValuesManage: 'Manage Sub-Partition Values',
    multilParTip: 'This model used multilevel partitioning, which are not supported at the moment. Please set subpartition as \'None\' in model partition dialog, or turn on \'Multilevel Partitioning\' in project settings.',
    streaming: 'Streaming',
    segmentHoletips: 'There exists a gap in the segment range, and the data of this range cannot be queried. Please confirm whether to add the following segments to fix.',
    fixSegmentTitle: 'Fix Segment'
  },
  'zh-cn': {
    capbility: '加速比例',
    dataLoadTime: '最近更新时间：',
    status: '状态',
    streamingTips: '实时模型，部分功能不可用',
    modelPartitionSet: '分区设置',
    enableModelTip: '你确认要上线模型 {modelName} 吗？',
    enableModelTitle: '上线模型',
    noModel: '您可以点击下面的按钮来添加模型',
    datacheck: '数据检测',
    favorite: '加速查询',
    importMdx: '导入 MDX',
    exportTds: '导出 TDS',
    exportMdx: '导出 MDX',
    rename: '重命名',
    delete: '删除',
    purge: '清空',
    disable: '禁用',
    enable: '启用',
    recommendationsTiTle: '优化建议',
    inputModelName: '请输入新的模型名字',
    inputCloneName: '请输入克隆模型的名字',
    segment: 'Segment',
    aggregateGroup: '聚合组',
    tableIndex: '明细索引',
    indexOverview: '索引总览',
    onLine: '模型上线',
    offLine: '模型下线',
    build: '构建索引',
    buildTitle: '构建',
    storage: '存储',
    usage: '使用量',
    rowCount: '行数',
    fullLoadDataTitle: '加载数据',
    fullLoadDataContent1: '模型 {modelName} 没有分区。',
    fullLoadDataContent2: '本次数据加载将包括本模型下所有的数据，总存储为 {storageSize}。',
    fullLoadDataContent3: '您确认需要继续加载数据吗？',
    fullScreen: '全屏',
    exitFullScreen: '退出全屏',
    usageTip: '查询击中该{mode}的次数',
    model: '模型',
    description: '描述',
    indexGroup: '索引组',
    expansionRate: '膨胀率',
    expansionRateTip: '膨胀率 = 存储数据大小/源表大小<br/>注：当存储数据小于 1GB 时，膨胀率不做显示。',
    tentative: '未知',
    recommendations: '优化建议',
    recommendationsTip: '系统将根据查询历史和模型使用情况，对当前模型的提供一些优化建议。',
    clearAll: '清除所有',
    ALL: '全选',
    ONLINE: 'ONLINE',
    OFFLINE: 'OFFLINE',
    BROKEN: 'BROKEN',
    status_c: '状态：',
    modelStatus_c: '模型状态：',
    modelType_c: '模型属性：',
    SECOND_STORAGE: '分层存储模型',
    sec_storage_model: '模型属性',
    others: '其他',
    reset: '重置',
    lastModifyTime_c: '最后更新时间：',
    lastBuildTime: '最近构建时间：',
    secStorage: '分层存储',
    allTimeRange: '全部时间范围',
    filterButton: '筛选',
    aggIndexCount: '索引数量',
    recommendations_c: '优化建议：',
    clickToView: '点击查看',
    filterModelOrOwner: '搜索模型名称',
    modelSegmentHoleTips: '当前 Segment 区间存在空洞，此时将无法查询到该段时间的数据。',
    autoFix: '自动修复',
    buildTips: '模型尚未构建索引，不可服务于查询分析。构建索引后可优化查询性能。',
    iKnow: '知道了',
    emptyIndexTips: '该模型中存在未构建的索引。请点击构建索引按钮以构建索引。',
    exportMetadata: '导出元数据',
    noModelsExport: '该项目中无任何模型,无法导出模型元数据。',
    exportMetadatas: '导出模型',
    exportMetadataSuccess: '正在生成模型元数据包。生成后将开始下载，请稍后。',
    exportMetadataFailed: '无法导出，请重试。',
    bokenModelExportMetadatasTip: '该模型状态为 BROKEN，无法导出',
    importModels: '导入模型',
    guideToAcceptRecom: '您可以点击模型右侧的提示符来接受系统根据查询历史生成的模型优化建议。',
    overview: '总览',
    changeModelOwner: '变更所有者',
    change: '变更',
    modelName: '模型',
    modelType: '模型类型',
    STREAMING: '实时模型',
    BATCH: '离线模型',
    HYBRID: '融合模型',
    changeTo: '变更所有者为',
    pleaseChangeOwner: '请选择模型所有者',
    changeDesc: '您可以将该模型的所有者变更为系统管理员、项目 ADMIN 角色的用户或者项目 Management 角色的用户。',
    buildIndex: '构建索引',
    batchBuildSubTitle: '请为新增的索引选择需要构建至的数据范围。',
    modelMetadataChangedTips: '当前模型源表发生变化，可能会影响查询结果的准确性。',
    seeDetail: '查看详情',
    // modelMetadataChangedDesc: '检测到以下 Segment 中源表数据可能发生了变化，将导致构建后数据不一致。请与系统管理员核实。<br/>建议刷新以下这些 Segment 确保数据的一致性。',
    noSegmentOnlineTip: '该模型尚未添加 Segment，不可服务于查询。请先添加 Segment 后再上线。',
    refrashWarningSegment: '仅支持刷新状态为 ONLINE 的 Segment',
    closeSCD2ModalOnlineTip: '该模型因存在 ≥ 或 < 的关联关系，当前不可上线。请删除相应关联关系，或在项目设置中开启支持拉链表开关。',
    SCD2ModalOfflineTip: '该模型中存在 ≥ 或 < 的关联关系，当前不可用。请删除相应关联关系，或在项目设置中开启支持拉链表开关。',
    storageTip: '模型下已构建数据的存储大小',
    subPartitionValuesManage: '子分区值设置',
    multilParTip: '该模型使用了多级分区，当前不可用。请在分区设置中将子分区列设为”无分区”，或在项目设置中开启支持多级分区。',
    streaming: '实时任务',
    segmentHoletips: '当前 Segment 区间存在空洞，此时将无法查询到该段时间的数据，是否需要补充以下 Segment 进行修复？',
    fixSegmentTitle: '修复 Segment'
  }
}
