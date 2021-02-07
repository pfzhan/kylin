export default {
  en: {
    storageSize: 'Storage Size',
    modifyTime: 'Last Updated Time',
    sourceRecords: 'Source Records',
    cannotBuildTips: 'Only ONLINE or WARNING segments could be refreshed.',
    failedSegmentsTips: 'Successfully submitted {sucNum} jobs. Can\'t submit {failNum} jobs.',
    details: 'The following segments can\'t be built as they might not exist or be locked at the moment. Please check and try again. ',
    failedTitle: 'Can\'t Submit Jobs',
    gotIt: 'Got It',
    noSegmentList: 'Not data range could be selected at the moment. There might be no segment existed, or all indexes might have been built to all segments.',
    parallelBuild: 'Build multiple segments in parallel',
    parallelBuildTip: 'By default, only one build job would be generated for all segments. With this option checked, multiple jobs would be generated and segments would be built in parallel.',
    subPratitionAmount: 'Subpartition Amount',
    subPratitionAmountTip: 'Amount of the built ones / Total amount'
  },
  'zh-cn': {
    storageSize: '存储大小',
    modifyTime: '最后更新时间',
    sourceRecords: '行数',
    cannotBuildTips: '仅支持刷新状态为 ONLINE 或 WARNING 的 Segment。',
    failedSegmentsTips: '{sucNum} 条任务提交成功，{failNum} 条任务无法提交。',
    details: '以下 segment 可能不存在或处于锁定状态，无法执行构建任务。请检查后再次提交。',
    failedTitle: '无法提交',
    gotIt: '知道了',
    noSegmentList: '暂无可选的数据范围。可能由于模型无 Segment，或选择的索引已构建至全部 Segment 中。',
    parallelBuild: '拆分多个任务并发构建',
    parallelBuildTip: '系统默认仅生成一个构建任务。勾选此选项后，将根据所选的 Segment 生成多个对应的任务，进行并发构建。',
    subPratitionAmount: '子分区数',
    subPratitionAmountTip: '已构建子分区数/子分区总数'
  }
}
