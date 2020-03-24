export default {
  'en': {
    capbility: 'Favorite Rate',
    dataLoadTime: 'Last Modified Time',
    status: 'Status',
    modelPartitionSet: 'Model Partition',
    delModelTip: 'Are you sure to drop the model {modelName}?',
    delModelTitle: 'Drop Model',
    pergeModelTip: 'Are you sure to purge the model {modelName}?',
    pergeModelTitle: 'Purge Model',
    disableModelTip: 'Are you sure to offline the model {modelName}?',
    disableModelTitle: 'Offline Model',
    enableModelTip: 'Are you sure to online the model {modelName}?',
    disableModelSuccessTip: 'Offline the model successfully.',
    enabledModelSuccessTip: 'Online the model successfully.',
    enableModelTitle: 'Online Model',
    purgeModelSuccessTip: 'Perge the model successfully.',
    deleteModelSuccessTip: 'Delete the model successfully.',
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
    inputModelName: 'Please input new model name',
    inputCloneName: 'Please input clone model name',
    segment: 'Segment',
    aggregateGroup: 'Aggregate Group',
    tableIndex: 'Table Index',
    indexOverview: 'Index Overview',
    onLine: 'Online',
    offLine: 'Offline',
    build: 'Build Index',
    storage: 'Storage',
    usage: 'Usage',
    fullLoadDataTitle: 'Load Data',
    fullLoadDataContent1: 'The model {modelName} has no partition.',
    fullLoadDataContent2: 'The load data job may reload its all data, which including {storageSize} storage.',
    fullLoadDataContent3: 'Do you still need to reload its data?',
    fullScreen: 'Full Screen',
    exitFullScreen: 'Exit Full Screen',
    usageTip: 'Times of the {mode} used by queries',
    model: 'model',
    indexGroup: 'index group',
    expansionRate: 'Expansion Rate',
    expansionRateTip: 'Expansion Rate = Storage Size / Source Table Size',
    tentative: 'Tentative',
    recommendations: 'Recommendation',
    recommendationsTip: 'By analyzing the query history and model usage, the system will provide some recommendations.',
    clearAll: 'Clear All',
    authorityDetails: 'The details of authority',
    ALL: 'All',
    ONLINE: 'ONLINE',
    OFFLINE: 'OFFLINE',
    BROKEN: 'BROKEN',
    status_c: 'Status:&nbsp;',
    modelStatus_c: 'Model Status:',
    reset: 'Reset',
    lastModifyTime_c: 'Last modified time:&nbsp;',
    allTimeRange: 'All Time Range',
    filterButton: 'Filter',
    aggIndexCount: 'Index Amount',
    recommendations_c: 'Recommendation: ',
    clickToView: 'Review',
    filterModelOrOwner: 'Search model name or owner',
    modelSegmentHoleTips: 'There exists a hole in the segment range, and the model will not be able to server queries.Try to ',
    autoFix: 'automatic fix',
    segmentHoletips: 'There exists a hole in the segment range, and the model will not be able to server queries. Please confirm whether to add the following segments to fix.',
    fixSegmentTitle: 'Fix Segment',
    ID: 'ID',
    column: 'Column',
    sort: 'Order',
    buildTips: 'The indexes in the model have not been built and are not available for query analysis. Build indexes to optimize query performance.',
    iKown: 'I Kown',
    emptyIndexTips: 'This model has unbuilt indexes. Please click the Load Data button to build the indexes.',
    noIndexTips: 'You should add indexes first before building.',
    exportMetadata: 'Export Metadata',
    noModelsExport: 'There are no models in this project, and model metadata cannot be exported.',
    exportMetadatas: 'Export Models',
    exportMetadataSuccess: 'A model metadata package is being generated. The download will start after generation. please wait.',
    exportMetadataFailed: 'Export models failed. Please try again.',
    importModels: 'Import Models'
  },
  'zh-cn': {
    capbility: '加速比例',
    dataLoadTime: '最近修改时间',
    status: '状态',
    modelPartitionSet: '分区设置',
    delModelTip: '你确认要删除模型 {modelName}？',
    delModelTitle: '删除模型',
    pergeModelTip: '你确定要清空模型 {modelName} 的数据吗？',
    pergeModelTitle: '清空模型',
    disableModelTip: '你确认要下线模型 {modelName} 吗？',
    disableModelTitle: '下线模型',
    enableModelTip: '你确认要上线模型 {modelName} 吗？',
    enableModelTitle: '上线模型',
    disableModelSuccessTip: '模型成功下线。',
    enabledModelSuccessTip: '模型成功上线。',
    purgeModelSuccessTip: '清理模型成功。',
    deleteModelSuccessTip: '删除模型成功。',
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
    inputModelName: '请输入新的模型名字',
    inputCloneName: '请输入克隆模型的名字',
    segment: 'Segment',
    aggregateGroup: '聚合组',
    tableIndex: '明细索引',
    indexOverview: '索引总览',
    onLine: '模型上线',
    offLine: '模型下线',
    build: '构建索引',
    storage: '存储大小',
    usage: '使用次数',
    fullLoadDataTitle: '加载数据',
    fullLoadDataContent1: '模型 {modelName} 没有分区。',
    fullLoadDataContent2: '本次数据加载将包括本模型下所有的数据，总存储为 {storageSize}。',
    fullLoadDataContent3: '您确认需要继续加载数据吗？',
    fullScreen: '全屏',
    exitFullScreen: '退出全屏',
    usageTip: '查询击中该 {mode} 的次数',
    model: '模型',
    indexGroup: '索引组',
    expansionRate: '膨胀率',
    expansionRateTip: '膨胀率 = 存储数据大小/源表大小',
    tentative: '未知',
    recommendations: '优化建议',
    recommendationsTip: '系统将根据查询历史和模型使用情况，对当前模型的提供一些优化建议。',
    clearAll: '清除所有',
    authorityDetails: '权限详情',
    ALL: '全选',
    ONLINE: 'ONLINE',
    OFFLINE: 'OFFLINE',
    BROKEN: 'BROKEN',
    status_c: '状态：',
    modelStatus_c: '模型状态：',
    reset: '重置',
    lastModifyTime_c: '修改时间：&nbsp;',
    allTimeRange: '全部时间范围',
    filterButton: '筛选',
    aggIndexCount: '索引数量',
    recommendations_c: '优化建议：',
    clickToView: '点击查看',
    filterModelOrOwner: '搜索模型名称或所有者',
    modelSegmentHoleTips: '当前Segment区间存在空洞，此时将无法服务于查询，尝试',
    autoFix: '自动修复',
    segmentHoletips: '当前 Segment 区间存在空洞，此时将无法服务于查询，是否需要补充以下 Segment 进行修复？',
    fixSegmentTitle: '修复 Segment',
    buildTips: '模型尚未构建索引，不可服务于查询分析。构建索引后可优化查询性能。',
    iKown: '知道了',
    emptyIndexTips: '该模型中存在未构建的索引。请点击加载数据按钮以构建索引。',
    noIndexTips: '您需要先添加索引，才可以进行构建。',
    exportMetadata: '导出元数据',
    noModelsExport: '该项目中无任何模型,无法导出模型元数据。',
    exportMetadatas: '导出模型',
    exportMetadataSuccess: '正在生成模型元数据包。生成后将开始下载，请稍后。',
    exportMetadataFailed: '导出失败，请重试。',
    importModels: '导入模型'
  }
}
