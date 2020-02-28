export default {
  'en': {
    dialogTitle: 'Diagnosis',
    generateBtn: 'Generate and download',
    timeRange: 'Select Time Range',
    timeRangeTip: 'The time range you choose should include when the failure occurred, based on the time of the server where the instance is located.',
    lastHour: 'Last one hour',
    lastDay: 'Last one day',
    lastThreeDay: 'Last three days',
    lastMonth: 'Last one month',
    custom: 'Customize',
    customTimeTip: 'The time range you can fill in is from five minutes to a month',
    selectDatePlaceholder: 'Select Date',
    server: 'Server：',
    downloadTip: 'After the diagnostic package is successfully generated, it will be downloaded automatically.',
    requireOverTime1: 'Request timeout. The {server} server failed to generate the diagnostic package. Please try again or refer to the ',
    requireOverTime2: ' to check diagnosis tools.',
    manual: 'user manual',
    noAuthorityTip: 'Permission denied. The {server} server failed to download the diagnostic package.',
    manualDownloadTip: 'If the diagnostic package fails to be downloaded, you can download it manually.',
    manualDownload: 'Download manually',
    download: 'Download',
    cancel: 'Cancel',
    details: 'Details',
    retry: 'Try again',
    selectAll: 'Select all',
    otherErrorMsg: 'Error occurred. The {server} server failed to download the diagnostic package. ',
    closeModelTip: 'The contents of this window will not be retained after closing, you can go to $ KYLIN_HOME/diag_dump/  to view the generated diagnostic package.',
    modelTitle: 'Notice',
    confrimBtn: 'Close',
    cancelBtn: 'Cancel',
    timeErrorMsg: 'Please fill in a reasonable time range',
    selectServerTip: 'Server is required',
    selectServerPlaceHolder: 'Please select server'
  },
  'zh-cn': {
    dialogTitle: '诊断',
    generateBtn: '生成并下载诊断包',
    timeRange: '时间范围',
    timeRangeTip: '您所选择的时间范围应包含故障发生的时间，以实例所在服务器的时间为准',
    lastHour: '最近一小时',
    lastDay: '最近一天',
    lastThreeDay: '最近三天',
    lastMonth: '最近一个月',
    custom: '自定义',
    customTimeTip: '可填写的时间范围为五分钟到一个月',
    selectDatePlaceholder: '选择日期时间',
    server: '服务器：',
    downloadTip: '诊断包成功生成后，将自动下载至本地。',
    requireOverTime1: '超时：{server} 服务器下载诊断包超时，请重试或参考',
    requireOverTime2: '通过脚本生成。',
    manual: '用户手册',
    noAuthorityTip: '权限不足，{server}服务器下载诊断包失败。',
    manualDownloadTip: '若下载诊断包失败，您可以手动下载',
    manualDownload: '手动下载',
    download: '下载',
    cancel: '取消',
    details: '详情',
    retry: '重试',
    selectAll: '选择全部',
    otherErrorMsg: '其他：{server} 服务器下载诊断包过程中发生错误。',
    closeModelTip: '关闭后该弹窗内的内容不会保留，您可到 $KYLIN_HOME/diag_dump/ 目录下查看已生成的诊断包。',
    modelTitle: '提示',
    confrimBtn: '确认关闭',
    cancelBtn: '取消',
    timeErrorMsg: '请填写合理的时间范围',
    selectServerTip: '请选择服务器',
    PREPARE: '打包请求中',
    EXTRACT: '正在提取打包信息',
    COMPRESS: '正在压缩',
    DONE: '打包完成后开始下载',
    selectServerPlaceHolder: '请选择服务器'
  }
}
