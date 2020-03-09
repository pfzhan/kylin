export default {
  'en': {
    'editAggregateGroup': 'Edit Aggregate Index',
    'addAggregateGroup': 'Add Aggregate Index',
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
    'clearAll': 'Clear all',
    'tooManyDimensions': 'No more than 62 dimensions used in all aggregate groups.',
    'delAggregateTip': 'Are you sure to delete Aggregate-Group-{aggId}?',
    'delAggregateTitle': 'Delete Aggregate Group',
    'clearAggregateTitle': 'Clear Dimensions',
    'clearAllAggregateTip': 'Are you sure to clear all dimensions of Aggregate-Group-{aggId}?',
    'maxCombinationTip': 'The aggregate amount exceeds its limit per aggregate group, please optimize the group setting or reduce dimension amount.',
    checkIndexAmount: 'Check Index Amount',
    checkIndexAmountBtnTips: 'Check if the index amount exceeds the upper limit',
    numTitle: 'Index Amount: {num}',
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
    clearAllMeasuresTip: 'Are you sure to clear all measures of Aggregate-Group-{aggId}?',
    clearMeasureTitle: 'Clear Measures',
    aggGroupTip: 'You can define dimensions and measures in different aggregate groups according to your business scenario. In the dimension setting, it is recommended to select the frequently used grouping dimensions and filtering dimensions into the aggregate group in the descending order of the cardinality.',
    increaseTips: 'After submitting, {increaseNum} index(indexes) will be added into model {model_name}. The index(indexes) can serve queries after being built. Please confirm whether to submit and build, or just submit without building?',
    decreaseTips: 'After submitting, {decreaseNum} index(indexes) will be deleted from model {model_name}. The index(indexes) cannot serve queries after being deleted, and the delete operation cannot be recovered. Are you sure to submit?',
    mixTips: 'After submitting, {decreaseNum} index(indexes) will be deleted and {increaseNum} index(indexes) will be added in model {model_name}. At this point, the influenced indexes will be changed to "LOCKED" status and can still serve queries. All the changes will take effect after the new indexes are built successfully. Please note that the deleted indexes cannot be recovered. Please confirm whether to submit and build, or just submit without building?',
    bulidAndSubmit: 'Submit and Build',
    maxDimCom: 'Max Dimension Combination',
    noLimitation: 'No Limitation',
    maxDimComTips: 'MDC (Max Dimension Combination) is the max number of dimensions in aggregate index. Please set this number carefully according to your query characteristics. The MDC set here applies to all aggregate groups without separate MDC.',
    dimConfirm: 'After confirmation, the MDC set here will be applied to all aggregate groups that have not set MDC seperately. Please confirm whether to apply?',
    dimComTips: 'The MDC set here applies only to this aggregation group.',
    calcError: 'Failed to check index amout. ',
    calcSuccess: 'Succeeded to check index amout. ',
    clearDimTips: 'Clear all MDC',
    disableClear: 'There is no MDC restriction in the current aggregated group(s) and no clearing is required.',
    clearConfirm: 'After confirmation, all MDCs will be cleared, and there will be no MDC restriction in all aggregate groups. Please confirm whether  to clear all MDCs?',
    clearbtn: 'Confirm to Clear',
    clearSuccess: 'Succeeded to clear all MDCs. ',
    needCheck: 'Pending'
  },
  'zh-cn': {
    'editAggregateGroup': '编辑聚合索引',
    'addAggregateGroup': '添加聚合索引',
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
    clearAllDimension: '清除所有维度',
    'clearAll': '清除所有',
    'tooManyDimensions': '在所有聚合组中，被使用的维度数量不能超过 62 个。',
    'delAggregateTip': '你确认要删除聚合组-{aggId}吗？',
    'delAggregateTitle': '删除聚合组',
    'clearAggregateTitle': '清除维度',
    'clearAllAggregateTip': '你确认要清除聚合组-{aggId}的所有维度吗？',
    'maxCombinationTip': '聚合索引数超过单个聚合组可以接受的上限，请优化当前聚合组或者减少聚合组中的维度。',
    checkIndexAmount: '检测索引数',
    checkIndexAmountBtnTips: '检测聚合索引数是否超过上限',
    numTitle: '索引数：{num}',
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
    increaseTips: '提交后模型 {model_name} 中将有 {increaseNum} 个索引被添加，新索引构建完成后将服务于查询。请确认是否提交并构建，或者仅提交不进行构建？',
    decreaseTips: '提交后模型 {model_name} 中将有 {decreaseNum} 个索引被删除，索引删除后不能服务于查询，且删除操作不可恢复。请确认是否提交？',
    mixTips: '提交后模型 {model_name} 中将有 {decreaseNum} 个索引被删除，{increaseNum} 个索引被添加。此时，受影响的索引将处于 "锁定" 状态，并仍可服务于查询。所有修改将在新索引构建完成后生效，请注意索引被删除后无法恢复。请确认是否提交并构建，或者仅提交不进行构建？',
    bulidAndSubmit: '提交并构建',
    maxDimCom: '最大维度组合数',
    noLimitation: '无限制',
    maxDimComTips: '最大维度组合数是聚合索引中所包含的最大维度个数，设置该数据可以有效控制多维度索引的构建和存储开销，请根据您的查询特征谨慎设置。此处设置的最大维度组合数将应用于所有未单独设置最大维度组合数的聚合组。',
    dimConfirm: '确认后所设最大维度组合数将应用于所有未设置过最大维度组合数的聚合组，请确认是否要应用？',
    dimComTips: '此处设置的最大维度组合数仅对该聚合组生效。',
    calcError: '检测索引数失败。',
    calcSuccess: '检测索引数成功。',
    clearDimTips: '清空所有最大维度组合数',
    disableClear: '当前聚合组内无最大维度组合数限制，无需清空。',
    clearConfirm: '确认后将清空全部最大维度组合数，所有的聚合组将失去已设最大维度组合数限制，请确认是否要清空？',
    clearbtn: '确认清空',
    clearSuccess: '清空全部最大维度组合数成功。',
    needCheck: '待检测'
  }
}
