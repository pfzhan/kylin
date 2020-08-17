export default {
  'en': {
    dialogTitle: 'Diagnosis',
    jobDiagnosis: 'Download Job Diagnostic Package',
    systemDiagnosis: 'Download System Diagnostic Package',
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
    downloadTip: 'After the diagnostic package is successfully generated, it will be downloaded automatically',
    requireOverTime1: 'Request timeout. The server failed to generate the diagnostic package. Please try again or refer to the ',
    requireOverTime2: ' to check diagnosis tools.',
    manual: 'user manual',
    noAuthorityTip: 'Permission denied. The server failed to download the diagnostic package.',
    manualDownloadTip: 'If the diagnostic package fails to be downloaded automatically, please download manually after checking the network connectivity and browser security settings.',
    manualDownload: 'Download manually',
    download: 'Download',
    cancel: 'Cancel',
    details: 'Details',
    retry: 'Try again',
    selectAll: 'Select all',
    otherErrorMsg: 'Error occurred. The server failed to download the diagnostic package. ',
    closeModelTip: 'After closing, the content in the window will not be retained, and the ongoing diagnostic package generation task(s) will be interrupted. You can go to $ KYLIN_HOME/diag_dump/  to view the generated diagnostic package.',
    modelTitle: 'Notice',
    confrimBtn: 'Close',
    cancelBtn: 'Cancel',
    timeErrorMsg: 'Please fill in a reasonable time range',
    selectServerTip: 'Server is required',
    selectServerPlaceHolder: 'Please select server',
    downloadJobDiagPackage1: 'For job errors, please download the job diagnostic package (including this job’s logs for Executor, Driver, and this project’s metadata).<br/>For other errors (i.e. query error), please download system diagnostic package in ',
    adminMode: 'admin mode',
    downloadJobDiagPackage2: '.',
    downloadJobDiagPackageForNorAdmin: 'For job errors, please download the job diagnostic package. It includes this job’s logs for Executor, Driver, and this project’s metadata.<br/>For other errors (i.e. query error), please contact your system admin to download system diagnostic package.',
    downloadSystemDiagPackage1: 'For errors excluding job errors, please download system diagnostic package. It includes the system’s metadata and logs.<br/>For job errors, please go to the ',
    jobPage: 'job page',
    downloadSystemDiagPackage2: ' to download the respective job diagnostic package.',
    downloadJobDiagPackage1ForIframe: 'For job errors, please download the job diagnostic package. It includes this job’s logs for Executor, Driver, and this project’s metadata.<br/>For other errors (i.e. failed to scale up/down cluster, query error), please download system diagnostic package in ',
    workspaceList: 'workspace list',
    downloadJobDiagPackage2ForIframe: '.',
    downloadJobDiagPackageForNorAdminForIframe: 'For job errors, please download the job diagnostic package. It includes this job’s logs for Executor, Driver, and this project’s metadata.<br/>For other errors (i.e. failed to scale up/down cluster, query error), please contact your system admin to download system diagnostic package.'
  },
  'zh-cn': {
    dialogTitle: '诊断',
    jobDiagnosis: '下载任务诊断包',
    systemDiagnosis: '下载系统诊断包',
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
    downloadTip: '诊断包成功生成后，将自动下载至本地',
    requireOverTime1: '超时：服务器下载诊断包超时，请重试或参考',
    requireOverTime2: '通过脚本生成。',
    manual: '用户手册',
    noAuthorityTip: '权限不足，服务器下载诊断包失败。',
    manualDownloadTip: '若自动下载失败，请检查网络连通性和浏览器安全设置后手动下载。',
    manualDownload: '手动下载',
    download: '下载',
    cancel: '取消',
    details: '详情',
    retry: '重试',
    selectAll: '选择全部',
    otherErrorMsg: '其他：服务器下载诊断包过程中发生错误。',
    closeModelTip: '关闭后该弹窗内的内容不会保留，仍在进行的诊断包生成任务将被中断，您可到 $KYLIN_HOME/diag_dump/ 目录下查看已生成的诊断包。',
    modelTitle: '提示',
    confrimBtn: '确认关闭',
    cancelBtn: '取消',
    timeErrorMsg: '请填写合理的时间范围',
    selectServerTip: '请选择服务器',
    PREPARE: '打包请求中',
    EXTRACT: '正在提取打包信息',
    COMPRESS: '正在压缩',
    DONE: '打包完成后开始下载',
    selectServerPlaceHolder: '请选择服务器',
    downloadJobDiagPackage1: '当任务报错需要诊断时，请下载任务诊断包，其中包括该任务的 Executor、Driver 日志和该项目元数据等。<br/>若有其他类型的报错（例如查询报错）需诊断，请前往',
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
