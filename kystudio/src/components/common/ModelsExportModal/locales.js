export default {
  'en': {
    exportModel: 'Export Model',
    chooseModels: 'Select Model',
    emptyText: 'No results',
    placeholder: 'Search by model name',
    factTable: 'Fact Table',
    exportSuccess: 'The model metadata package(s) is being generated. Download will start automatically once ready.',
    exportFailed: 'Can\'t export models at the moment. Please try again.',
    export: 'Export',
    fetchModelsFailed: 'Can\'t fetch the model list.',
    exportAllTip: 'To ensure the file could be imported, please don\'t unzip the file or modify the contents.',
    exportOneModelTip: 'The model\'s metadata would be exported. It includes referenced tables, table relationships, partition columns, filter conditions, measures, dimensions, computed columns, and indexes.',
    exportOther: 'Select other contents to be exported',
    exportOtherTips: 'The selected content will be included when overwriting or adding new models during import.',
    recommendations: 'Model\'s recommendations',
    override: 'Model rewrite settings',
    loading: 'Loading...',
    disabledRecommendationTip: 'No recommendations for the selected model(s)',
    disabledOverrideTip: 'No overrides for the selected model(s)',
    exportBrokenModelCheckboxTip: 'Can\'t export model file at the moment as the model is BROKEN',
    subPartitionValues: 'Sub partition values',
    disabledMultPartitionTip: 'No subpartitions included in the selected model(s)'
  },
  'zh-cn': {
    exportModel: '导出模型',
    chooseModels: '选择模型',
    emptyText: '搜索结果为空',
    placeholder: '搜索模型名称',
    factTable: '事实表',
    exportSuccess: '正在生成模型元数据包。生成后将开始下载，请稍后。',
    exportFailed: '无法导出，请重试。',
    export: '导出',
    fetchModelsFailed: '无法获取模型列表。',
    exportAllTip: '为了确保文件完整性，请勿解压文件或修改文件内容。',
    exportOneModelTip: '导出模型的元数据，包括模型引用的表、表关系、分区列、可计算列、过滤条件、度量、维度、索引。',
    exportOther: '选择导出的其他内容',
    exportOtherTips: '导入时覆盖或者新增模型将包括选中内容。',
    recommendations: '优化建议',
    override: '重写设置',
    loading: '加载中...',
    disabledRecommendationTip: '所选模型无优化建议',
    disabledOverrideTip: '所选模型无重写设置',
    exportBrokenModelCheckboxTip: '该模型状态为 BROKEN，无法导出',
    subPartitionValues: '子分区值',
    disabledMultPartitionTip: '所选模型均无子分区值'
  }
}
