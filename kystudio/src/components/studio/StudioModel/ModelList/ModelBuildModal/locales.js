export default {
  'en': {
    dataRangeValValid: 'Please input build date range',
    modelBuild: 'Load Data',
    buildRange: 'Build Range',
    startDate: 'Start Date',
    endDate: 'End Date',
    to: 'To',
    loadExistingData: 'Load existing data',
    loadExistingDataDesc: 'Load new records existing from the last load job.',
    customLoadRange: 'Custom Load Range',
    detectAvailableRange: 'Detect available range',
    invaildDate: 'Please input vaild date.',
    overlapsTips: 'The load data range overlaps with current segments. Please check and modify.',
    segmentHoletips: 'There exists a hole in the segment range, and the model will not be able to server queries. Please confirm whether to add the following segments to fix.',
    fixSegmentTitle: 'Fix Segment',
    ignore: 'Ignore',
    fixAndBuild: 'Fix and Build'
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
    segmentHoletips: '当前 Segment 区间存在空洞，此时将无法服务于查询，是否需要补充以下 Segment 进行修复？',
    fixSegmentTitle: '修复 Segment',
    ignore: '忽略',
    fixAndBuild: '修复并构建'
  }
}
