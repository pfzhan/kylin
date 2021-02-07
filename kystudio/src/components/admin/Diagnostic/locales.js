export default {
  'en': {
    dialogTitle: 'Diagnosis',
    jobDiagnosis: 'Download Job Diagnostic Package',
    systemDiagnosis: 'Download System Diagnostic Package',
    generateBtn: 'Generate and Download',
    timeRange: 'Select Time Range',
    timeRangeTip: 'The selected time range should include the time when the failure occurred, based on the system time of the server node.',
    lastHour: 'Last 1 Hour',
    lastDay: 'Last 1 Day',
    lastThreeDay: 'Last 3 Days',
    lastMonth: 'Last 1 Month',
    custom: 'Customize',
    customTimeTip: 'The time range could vary from 5 minutes to 1 month.',
    selectDatePlaceholder: 'Select Date',
    server: 'Server: ',
    downloadTip: 'The diagnostic package would be downloaded automatically once it\'s successfully generated.',
    requireOverTime1: 'Request timeout. The server can\'t generate the diagnostic package. Please try again or refer to the ',
    requireOverTime2: ' to check diagnosis tools.',
    manual: 'user manual',
    noAuthorityTip: 'Don\'t have permission to download the diagnostic package.',
    manualDownloadTip: 'If the diagnostic package can’t download automatically, please download it manually after checking network connectivity and browser security settings.',
    manualDownload: 'Download Manually',
    download: 'Download',
    cancel: 'Cancel',
    details: 'Details',
    retry: 'Try again',
    selectAll: 'Select All',
    otherErrorMsg: 'An error occurred when downloading the diagnostic package.',
    closeModelTip: 'The ongoing generating diagnostic package job(s) would be discarded after closing this window. You could go to $ KYLIN_HOME/diag_dump/ to view the generated diagnostic packages.',
    modelTitle: 'Notice',
    confrimBtn: 'Close Anyway',
    cancelBtn: 'Cancel',
    timeErrorMsg: 'The time range must be greater than or equal to 5 minutes and less than or equal  to 1 month. Please reselect.',
    selectServerTip: 'Please select the server(s)',
    selectServerPlaceHolder: 'Please select server',
    downloadJobDiagPackage1: 'For job errors, please download the job diagnostic package (including this job\'s logs for Executor, Driver, and this project\'s metadata).<br/>For other errors (i.e. query error), please download system diagnostic package in ',
    adminMode: 'admin mode',
    downloadJobDiagPackage2: '.',
    downloadJobDiagPackageForNorAdmin: 'For job errors, please download the job diagnostic package. It includes this job\'s logs for Executor, Driver, and this project\'s metadata.<br/>For other errors (i.e. query error), please contact your system admin to download system diagnostic package.',
    downloadSystemDiagPackage1: 'For errors excluding job errors, please download system diagnostic package. It includes the system\'s metadata and logs.<br/>For job errors, please go to the ',
    jobPage: 'job page',
    downloadSystemDiagPackage2: ' to download the respective job diagnostic package.',
    downloadJobDiagPackage1ForIframe: 'For job errors, please download the job diagnostic package. It includes this job\'s logs for Executor, Driver, and this project\'s metadata.<br/>For other errors (i.e. can\'t scale up/down cluster, query error), please download system diagnostic package in ',
    workspaceList: 'workspace list',
    downloadJobDiagPackage2ForIframe: '.',
    downloadJobDiagPackageForNorAdminForIframe: 'For job errors, please download the job diagnostic package. It includes this job\'s logs for Executor, Driver, and this project\'s metadata.<br/>For other errors (i.e. can\'t scale up/down cluster, query error), please contact your system admin to download system diagnostic package.'
  },
  'zh-cn': {
    dialogTitle: '诊断',
    jobDiagnosis: '下载任务诊断包',
    systemDiagnosis: '下载系统诊断包',
    generateBtn: '生成并下载诊断包',
    timeRange: '时间范围',
    timeRangeTip: '您所选择的时间范围应包含故障发生的时间，以实例所在服务器的时间为准。',
    lastHour: '最近 1 小时',
    lastDay: '最近 1 天',
    lastThreeDay: '最近 3 天',
    lastMonth: '最近 1 个月',
    custom: '自定义',
    customTimeTip: '可选择的时间范围为 5 分钟到 1 个月。',
    selectDatePlaceholder: '选择日期时间',
    server: '服务器：',
    downloadTip: '诊断包成功生成后，将被自动下载至本地。',
    requireOverTime1: '超时：服务器下载诊断包超时，请重试或参考',
    requireOverTime2: '通过脚本生成。',
    manual: '用户手册',
    noAuthorityTip: '权限不足，服务器下载诊断包失败。',
    manualDownloadTip: '如果自动下载诊断包失败，请检查网络连通性和浏览器安全设置后进行手动下载。',
    manualDownload: '手动下载',
    download: '下载',
    cancel: '取消',
    details: '详情',
    retry: '重试',
    selectAll: '选择全部',
    otherErrorMsg: '下载诊断包过程中发生错误。',
    closeModelTip: '如果关闭弹窗，正在进行的诊断包生成任务将被中断。您可以到 $KYLIN_HOME/diag_dump/ 目录下查看已生成的诊断包。',
    modelTitle: '提示',
    confrimBtn: '确定关闭',
    cancelBtn: '取消',
    timeErrorMsg: '时间范围必须大于等于 5 分钟，小于等于 1 个月。请重新选择。',
    selectServerTip: '请选择服务器',
    PREPARE: '打包请求中',
    EXTRACT: '正在提取打包信息',
    COMPRESS: '正在压缩',
    DONE: '打包完成后开始下载',
    selectServerPlaceHolder: '请选择服务器',
    downloadJobDiagPackage1: '当任务报错需要诊断时，请下载任务诊断包（包括该任务的 Executor、Driver 日志和该项目元数据等）。若任务正在运行，请先终止任务。<br/>其他错误（例如查询报错）请前往',
    adminMode: '系统管理页面',
    downloadJobDiagPackage2: '下载系统诊断包。',
    downloadJobDiagPackageForNorAdmin: '当任务报错需要诊断时，请下载任务诊断包，其中包括该任务的 Executor、Driver 日志和该项目元数据等。<br/>若有其他类型的报错（例如查询报错）需诊断，请联系您的系统管理员下载系统诊断包。',
    downloadSystemDiagPackage1: '当有除任务报错之外的错误需要诊断时，请下载系统诊断包，其中包括系统元数据、系统日志。<br/>如需诊断具体任务的报错，请前往',
    jobPage: '任务页面',
    downloadSystemDiagPackage2: '下载相应的任务诊断包。',
    downloadJobDiagPackage1ForIframe: '当任务报错需要诊断时，请下载任务诊断包，其中包括该任务的 Executor、Driver 日志和该项目元数据等。<br/>若有其他类型的报错（例如集群扩缩容失败、查询报错）需诊断，请前往',
    workspaceList: '工作区管理页面',
    downloadJobDiagPackage2ForIframe: '下载系统诊断包。',
    downloadJobDiagPackageForNorAdminForIframe: '当任务报错需要诊断时，请下载任务诊断包，其中包括该任务的 Executor、Driver 日志和该项目元数据等。<br/>若有其他类型的报错（例如集群扩缩容失败、查询报错）需诊断，请联系您的系统管理员下载系统诊断包。'
  }
}
