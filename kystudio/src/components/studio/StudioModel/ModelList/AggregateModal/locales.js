export default {
  'en': {
    'editAggregateGroup': 'Edit Aggregate Group',
    'addAggregateGroup': 'Add Aggregate Group',
    'hideDimensions': 'Hide Dimensions',
    'showDimensions': 'View All Dimensions',
    'aggregateGroupTitle': 'Aggregate-Group-{id}',
    'include': 'Include',
    'mandatory': 'Mandatory',
    'hierarchy': 'Hierarchy',
    'joint': 'Joint',
    'includesEmpty': 'Each aggregation group can\'t be empty.',
    'selectAllDimension': 'Select all dimensions',
    selectAllMeasure: 'Select all measures',
    clearAllMeasures: 'Clear all measures',
    clearAllDimension: 'Clear all dimensions',
    'clearAll': 'Clear All',
    'tooManyDimensions': 'No more than 62 dimensions used in all aggregate groups.',
    'delAggregateTip': 'Are you sure you want to delete aggregate group "{aggId}"?',
    'delAggregateTitle': 'Delete Aggregate Group',
    'clearAggregateTitle': 'Clear Dimensions',
    'clearAllAggregateTip': 'Are you sure you want to clear all dimensions of Aggregate-Group-{aggId}?',
    'maxCombinationTip': 'The number of aggregate indexes exceeds the limit of each aggregate group. Please try removing some dimensions, or adding some as mandatory/hierarchy/join if possible.',
    checkIndexAmount: 'Check Index Amount',
    checkIndexAmountBtnTips: 'Check if the index amount exceeds the upper limit',
    numTitle: 'Index Amount: {num}',
    numTitle1: 'Index Amount: ',
    exceedLimitTitle: 'Index Amount: Exceed Limit',
    maxTotalCombinationTip: 'The aggregate index amount exceeds the upper limit, please optimize the group setting or reduce dimension amount.',
    maxCombinationTotalNum: 'The upper limit of index amount per aggregate group is {num}. The upper limit of aggregate index amount is {numTotal}.',
    maxCombinationNum: 'The upper limit of index amount per aggregate group is {num}',
    dimension: 'Dimension（{size}/{total}）',
    measure: 'Measure（{size}/{total}）',
    retract: 'retract',
    open: 'open',
    includeMeasure: 'Include Measure',
    dimensionTabTip: 'Select all dimensions defined in the model into the aggregate group.',
    measureTabTip: 'Select all measures defined in the model into the aggregate group.',
    clearAllMeasuresTip: 'Are you sure you want to clear all measure of the aggregate group {aggId}?',
    clearMeasureTitle: 'Clear Measures',
    aggGroupTip: 'You can define dimensions and measures in different aggregate groups according to your business scenario. In the dimension setting, it is recommended to select the frequently used grouping dimensions and filtering dimensions into the aggregate group in the descending order of the cardinality.',
    increaseTips: 'After submission, {increaseNum} indexes would be added.',
    decreaseTips: 'After submission, {decreaseNum} indexes would be deleted.',
    mixTips: 'After submission, {decreaseNum} indexes would be deleted and {increaseNum} indexes would be added. Some indexes might be in "locked" state while still could answer queries.',
    onlyIncreaseOrDecreaseTip: 'After submission, {decreaseNum} indexes would be deleted and {increaseNum} indexes would be added.',
    confirmTextBySaveAndBuild: 'Do you want to save and build?',
    confirmTextBySave: 'Do you want save?',
    habirdModelBuildTips: '(Only batch index(es) will be built right now. Streaming index(es) will be built when streaming job is started again. )',
    bulidAndSubmit: 'Save and Build',
    maxDimCom: 'Max Dimension Combination',
    generateDeletedIndexes: 'Skip generating the {rollbackNum} deleted indexes',
    noLimitation: 'No Limitation',
    maxDimComTips: 'MDC (Max Dimension Combination) is the maximum amount of dimensions in the aggregate index. Please set this number carefully according to your query requirements. The MDC set here applies to all aggregate groups without separate MDC.',
    dimConfirm: 'The MDC set here would be applied to all aggregate groups which haven\'t set MDC seperately. Are you sure you want to apply?',
    dimComTips: 'The MDC set here applies only to this aggregation group',
    calcError: 'Can\'t check the index amount at the moment. ',
    calcSuccess: 'Successfully checked index amount. ',
    clearDimTips: 'Clear all MDC',
    disableClear: 'Can\'t clear as there is no MDC set.',
    clearConfirm: 'MDCs would be cleared for all aggregate groups. Are you sure you want to clear?',
    clearbtn: 'Confirm to Clear',
    clearSuccess: 'Successfully cleared all MDCs. ',
    needCheck: 'Pending',
    mandatoryDesc: 'If a dimension is set to a mandatory dimension, only the index with this dimension will be calculated.',
    hierarchyDesc: 'If there is a hierarchical relationship between the dimensions, you can set it as a hierarchy dimension.',
    jointDesc: 'For dimensions which are often used together and the possible combination in between doesn\'t really matter, you may add them as joint dimension. If sampling for the source table has been done, the product of cardinalities of joint dimensions would be calculated as a reference.',
    saveAndBuild: 'Save and Build Index',
    colon: ': ',
    edit: 'Edit',
    column: 'Column',
    statistics: 'Statistics',
    sample: 'Sample',
    property: 'Property',
    measureName: 'Measure',
    expression: 'Expression',
    parameters: 'Parameters',
    returnType: 'Return Type',
    noSelectDimensionTip: 'The selected dimension (column)\'s statistics and sample data would be shown here.',
    noSamplingTip: 'The source table of the selected dimension (column) hasn\'t been sampled yet. Neither statistics nor sample data is available. Please sample in the source page.',
    noSelectMeasureTip: 'The selected measure\'s properties would be shown here.',
    ccDimensionTip: 'Neither statistics nor sample data is available for the selected dimension (column).',
    editIncludeDimensions: 'Edit Included Dimensions',
    editIncludeDimensionTip: 'To enhance the query performance, please move the frequently used dimensions to the top of the list, in descending order of cardinality.',
    editMeasuresTip: 'Please select columns for detail query. To enhance the query performance, please move the frequently used dimensions to the top of the list, and set a column with relatively large cardinality as ShardBy.',
    searchIncludeDimension: 'Search dimension or column name',
    pleaseFilterNameOrColumn: 'Search by name or column',
    pleaseFilterName: 'Search by name',
    moveTop: 'Move to top',
    moveUp: 'Move up',
    moveDown: 'Move down',
    save: 'Save',
    th_name: 'Name',
    th_column: 'Column',
    th_dataType: 'Data Type',
    th_num: 'Cardinality',
    th_info: 'Comment',
    th_order: 'Order',
    ok: 'OK',
    cardinality: 'Cardinality',
    editMeasure: 'Edit Measures',
    max_length_value: 'Max Length Value',
    max_value: 'Max Value',
    min_length_value: 'Min Length Value',
    min_value: 'Min Value',
    null_count: 'Null Count',
    cardinalityMultiple: 'The Product of Cardinality: ',
    noIncludesTip: 'No included dimensions.',
    buildIndexTip: 'Successfully saved the aggregate index(es). The building job can\'t be submitted at the moment, as there exists an ongoing building job for this model. Please try submitting the building job until the current one is completed or manually stop it.',
    disabledConstantMeasureTip: 'Can\'t modify the default measure.',
    excludeTableCheckbox: 'Display columns excluded from recommendations',
    excludeTableCheckboxTip: 'Exclude Rules can be modified in project setting',
    excludedTableIconTip: 'Excluded from recommendations',
    indexTimeRange: 'Index’s Time Range',
    manyToManyAntiTableTip: 'For the tables excluded from recommendations, if the join relationship of a table is One-to-Many or Many-to-Many, dimensions from this table can\'t be used in indexes. ',
    indexTimeRangeTips: 'The data range that the indexes will be built in. With “Batch and Streaming“ selected, there will be generated batch indexes and streaming indexes with same content respectively. ',
    refuseAddIndexTip: 'Can\'t add streaming indexes. Please stop the streaming job and then delete all the streaming segments.'
  },
  'zh-cn': {
    'editAggregateGroup': '编辑聚合组',
    'addAggregateGroup': '添加聚合组',
    'hideDimensions': '隐藏维度',
    'showDimensions': '展示所有维度',
    'aggregateGroupTitle': '聚合组—{id}',
    'include': '包含的维度',
    'mandatory': '必需维度',
    'hierarchy': '层级维度',
    'joint': '联合维度',
    'includesEmpty': '任意聚合组不能为空。',
    'selectAllDimension': '选择所有维度',
    selectAllMeasure: '选择所有度量',
    clearAllMeasures: '清除所有度量',
    clearAllDimension: '清空所有维度',
    'clearAll': '清空',
    'tooManyDimensions': '在所有聚合组中，被使用的维度数量不能超过 62 个。',
    'delAggregateTip': '确定要删除聚合组 “{aggId}” 吗？',
    'delAggregateTitle': '删除聚合组',
    'clearAggregateTitle': '清除维度',
    'clearAllAggregateTip': '你确认要清除聚合组-{aggId}的所有维度吗？',
    'maxCombinationTip': '聚合索引数超过单个聚合组可以接受的上限，请优化当前聚合组或者减少聚合组中的维度。',
    checkIndexAmount: '检测索引数',
    checkIndexAmountBtnTips: '检测聚合索引数是否超过上限',
    numTitle: '索引数：{num}',
    numTitle1: '索引数：',
    exceedLimitTitle: '索引数：超过上限',
    maxTotalCombinationTip: '聚合索引总数超过上限，请优化当前聚合组或者减少聚合组中的维度。',
    maxCombinationTotalNum: '单个聚合组内索引上限为 {num}，聚合索引总数上限为 {numTotal}。',
    maxCombinationNum: '单个聚合组内索引上限为 {num}',
    dimension: '维度（{size}/{total}）',
    measure: '度量（{size}/{total}）',
    retract: '收起',
    open: '展开',
    includeMeasure: '包含的度量',
    dimensionTabTip: '将模型中定义的所有维度选入聚合组。',
    measureTabTip: '将模型中定义的所有度量选入聚合组。',
    clearAllMeasuresTip: '你确认要清除聚合组-{aggId}的所有度量吗',
    clearMeasureTitle: '清除度量',
    aggGroupTip: '您可以将维度和度量根据分析场景定义在不同的聚合组。在维度设置中，推荐将查询中常用的分组维度、过滤维度按照基数从大到小的方式选入聚合组。',
    increaseTips: '提交后，将新增 {increaseNum} 个索引。',
    decreaseTips: '提交后，将删除 {decreaseNum} 个索引。',
    mixTips: '提交后，将删除 {decreaseNum} 个索引，新增 {increaseNum} 个索引。部分索引可能处于“锁定”状态，但仍可服务查询。',
    onlyIncreaseOrDecreaseTip: '提交后，将删除 {decreaseNum} 个索引，新增 {increaseNum} 个索引。',
    confirmTextBySaveAndBuild: '是否继续保存并构建？',
    confirmTextBySave: '是否继续保存？',
    habirdModelBuildTips: '（将仅构建离线索引，实时索引的构建将在实时任务重新启动时进行。）',
    bulidAndSubmit: '保存并构建',
    maxDimCom: '最大维度组合数',
    generateDeletedIndexes: '不生成已删除的 {rollbackNum} 个索引',
    noLimitation: '无限制',
    maxDimComTips: '最大维度组合数是聚合索引中所包含的最大维度个数，设置该数据可以有效控制多维度索引的构建和存储开销，请根据您的查询特征谨慎设置。此处设置的最大维度组合数将应用于所有未单独设置最大维度组合数的聚合组。',
    dimConfirm: '确认后所设最大维度组合数将应用于所有未设置过最大维度组合数的聚合组，请确认是否要应用？',
    dimComTips: '此处设置的最大维度组合数仅对该聚合组生效',
    calcError: '无法检测索引数。',
    calcSuccess: '检测索引数成功。',
    clearDimTips: '清空所有最大维度组合数',
    disableClear: '当前聚合组内无最大维度组合数限制，无需清空。',
    clearConfirm: '确认后将清空全部最大维度组合数，所有的聚合组将失去已设最大维度组合数限制，请确认是否要清空？',
    clearbtn: '确认清空',
    clearSuccess: '清空全部最大维度组合数成功。',
    needCheck: '待检测',
    mandatoryDesc: '若有维度被设置成必需维度，则只有包含此维度的索引会被生成。',
    hierarchyDesc: '若维度间存在层级关系，您可以将其设置为层级维度。',
    jointDesc: '在查询中常被一起使用，但是并不需要关心互相之间各种组合方式的维度可加入联合维度。若开启表抽样，会自动计算基数乘积，供设置时参考。',
    saveAndBuild: '保存并构建',
    colon: '：',
    edit: '编辑',
    column: '列',
    statistics: '特征数据',
    sample: '采样数据',
    property: '属性',
    measureName: '度量名称',
    expression: '表达式',
    parameters: '参数',
    returnType: '返回类型',
    noSelectDimensionTip: '选中维度（列）的特征数据和采样数据会显示在这里。',
    noSamplingTip: '该维度（列）所在的表未开启抽样，特征数据和采样数据无法获取。请在数据源中进行抽样。',
    ccDimensionTip: '该维度（列）没有可显示的特征数据和采样数据。',
    noSelectMeasureTip: '选中度量的属性会显示在这里。',
    editIncludeDimensions: '编辑包含维度',
    editIncludeDimensionTip: '请将常用的维度按基数从大到小排列在前面，以提高查询效率。',
    editMeasuresTip: '请选择添加哪些列用于回答明细查询。为了提高查询效率，请将查询中常用的列排在前面，并将基数较大的设置为 ShardBy 列。',
    searchIncludeDimension: '搜索维度名称或列名',
    pleaseFilterNameOrColumn: '搜索名称或列名',
    pleaseFilterName: '搜索名称',
    moveTop: '置顶',
    moveUp: '上移',
    moveDown: '下移',
    save: '保存',
    th_name: '名称',
    th_column: '列',
    th_dataType: '数据类型',
    th_num: '基数',
    th_info: '注释',
    th_order: '排序',
    ok: '确定',
    cardinality: '基数',
    editMeasure: '编辑度量',
    max_length_value: '最大长度值',
    max_value: '最大值',
    min_length_value: '最小长度值',
    min_value: '最小值',
    null_count: '空值计数',
    cardinalityMultiple: '基数乘积：',
    noIncludesTip: '当前没有添加包含维度。',
    buildIndexTip: '聚合索引保存成功，当前无法提交构建任务。请等待该模型的构建任务完成或手动停止该任务后再提交。',
    disabledConstantMeasureTip: '默认度量，暂不支持编辑和删除。',
    excludeTableCheckbox: '显示在优化建议中被屏蔽的列',
    excludeTableCheckboxTip: '可在项目设置中配置',
    excludedTableIconTip: '被优化建议屏蔽的列',
    indexTimeRange: '索引时间范围',
    manyToManyAntiTableTip: '在优化建议中被屏蔽的表，若关联关系为一对多或多对多，则表中的维度无法在索引中使用。',
    indexTimeRangeTips: '索引构建的数据范围。选择“离线和实时”将分别生成相同内容的实时和离线索引。',
    refuseAddIndexTip: '无法添加实时索引。请先停止实时任务，再清空实时 Segment。'
  }
}
