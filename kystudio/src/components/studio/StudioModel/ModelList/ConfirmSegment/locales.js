export default {
  en: {
    storageSize: 'Storage Size',
    modifyTime: 'Last Modified Time',
    sourceRecords: 'Source Records',
    cannotBuildTips: 'Only ONLINE or WARNING segments could be refreshed.',
    failedSegmentsTips: 'Successfully submitted {sucNum} jobs. Failed to submit {failNum} jobs.',
    details: 'The following segments can’t be built as they might not exist or be locked at the moment. Please check and try again. ',
    failedTitle: 'Can’t Submit Jobs',
    gotIt: 'Got It',
    noSegmentList: 'No data range. Please add a segment for building index.'
  },
  'zh-cn': {
    storageSize: '存储大小',
    modifyTime: '最后修改时间',
    sourceRecords: '行数',
    cannotBuildTips: '仅支持刷新状态为 ONLINE 或 WARNING 的 Segment。',
    failedSegmentsTips: '{sucNum} 条任务提交成功，{failNum} 条任务提交失败。',
    details: '以下 segment 可能不存在或处于锁定状态，无法执行构建任务。请检查后再次提交。',
    failedTitle: '提交失败',
    gotIt: '知道了',
    noSegmentList: '暂无可选的数据范围，请先添加一个 Segment 后再构建索引。'
  }
}
