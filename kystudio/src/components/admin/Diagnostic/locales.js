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
    server: 'Server',
    downloadTip: 'The diagnostic package would be downloaded automatically once it\'s successfully generated.',
    requireOverTime1: 'Generate timeout. Please ',
    requireOverTime2: ' or refer to the ',
    requireOverTime3: ' to check diagnosis tools.',
    manual: 'user manual',
    noAuthorityTip: 'Don\'t have permission to download the diagnostic package.',
    manualDownloadTip: 'If the automatic download fails, please check your network connectivity and browser security settings, then click ',
    manualDownload: 'Download Manually',
    download: 'Download',
    cancel: 'Cancel',
    details: 'Details',
    retry: 'retry',
    selectAll: 'Select All',
    otherErrorMsg: 'An error occurred when downloading the diagnostic package.',
    closeModelTip: 'You can go to $KYLIN-HOME/diag_dump/ to see the currently generated diagnostic packages.',
    closeModalTitle: 'The diagnostic package will be lost when you continue, do you want to continue?',
    modelTitle: 'Notice',
    confrimBtn: 'Discard',
    cancelBtn: 'Cancel',
    timeErrorMsg: 'The time range must be greater than or equal to 5 minutes and less than or equal  to 1 month. Please reselect.',
    selectServerTip: 'Please select the server(s)',
    selectServerPlaceHolder: 'Please select server',
    downloadJobDiagPackage1: 'The diagnostic package includes this job’s logs for the executor, driver and project metadata. For other error like query error, please download system diagnostic package in ',
    adminMode: 'admin mode',
    downloadJobDiagPackage2: '.',
    downloadJobDiagPackageForNorAdmin: 'The diagnostic package includes this job’s logs for Executor, Driver and project metadata. For other error like query error, please contact your system admin to download system diagnostic package.',
    downloadSystemDiagPackage1: 'For errors excluding job errors, please download system diagnostic package. It includes the system\'s metadata and logs.<br/>For job errors, please go to the ',
    jobPage: 'job page',
    downloadSystemDiagPackage2: ' to download the respective job diagnostic package.',
    downloadJobDiagPackage1ForIframe: 'The diagnostic package includes this job’s logs for the executor, driver and project metadata. For other error like query error or cluster scaling failure, please download system diagnostic package in ',
    workspaceList: 'workspace list',
    downloadJobDiagPackage2ForIframe: '.',
    downloadJobDiagPackageForNorAdminForIframe: 'The diagnostic package includes this job’s logs for Executor, Driver and project metadata. For other error like query error or cluster scaling failure, please contact your system admin to download system diagnostic package.',
    downloadJobDiagPackage3: 'The job is running, it may cause the diagnostic package to be incomplete. It’s recommended to discard the job first.',
    monitor: 'monitor',
    queryDiagnostic: 'Download Query Diagnostic Package',
    downloadQueryDiagnostic: 'The diagnostic package includes this query’s logs for the executor, driver and project metadata, it may not be complete due to the system limitation. Job error, please download the job diagnostic package in ',
    downloadQueryDiagnosticForKEsubText1: '. For other error, please download system diagnostic package in ',
    downloadQueryDiagnosticForKEsubText2: '.',
    downloadQueryDiagnosticForKEsubText3: '. For other error, please contact your system admin to download system diagnostic package.',
    downloadQueryDiagnosticForKCsubText1: '. For other error like cluster scaling failure, please download system diagnostic package in ',
    deleteDiagnosticSuccess: 'Diagnostic package discarded',
    createDiagnostic: 'Generate diagnostic packages',
    queryPage: 'query history',
    downloadJobDiagnostic: 'The diagnostic package includes this job’s logs for the executor, driver and project metadata, it may not be complete due to the system limitation. Query error, please download the query diagnostic package in ',
    downloadJobDiagnosticSubText1: '. For other error, please download system diagnostic package in ',
    downloadJobDiagnosticSubText2: '. For other error, please contact your system admin to download system diagnostic package.',
    downloadJobDiagnosticSubText3: 'The diagnostic package includes this job’s logs for executor, driver and project metadata, it  may not be complete due to the system limitation. ',
    downloadJobDiagnosticSubText4: 'For other error like cluster scaling failure, please download system diagnostic package in ',
    downloadJobDiagnosticSubText5: 'For other error like cluster scaling failure, please contact your system admin to download system diagnostic package.'
  },
  'zh-cn': {
    dialogTitle: '诊断',
    jobDiagnosis: '下载任务诊断包',
    systemDiagnosis: '下载系统诊断包',
    generateBtn: '生成并下载',
    timeRange: '时间范围',
    timeRangeTip: '您所选择的时间范围应包含故障发生的时间，以实例所在服务器的时间为准。',
    lastHour: '最近 1 小时',
    lastDay: '最近 1 天',
    lastThreeDay: '最近 3 天',
    lastMonth: '最近 1 个月',
    custom: '自定义',
    customTimeTip: '可选择的时间范围为 5 分钟到 1 个月。',
    selectDatePlaceholder: '选择日期时间',
    server: '服务器',
    downloadTip: '诊断包成功生成后，将被自动下载至本地。',
    requireOverTime1: '生成超时，请 ',
    requireOverTime2: ' 或参考 ',
    requireOverTime3: ' 通过脚本生成。',
    manual: '用户手册',
    noAuthorityTip: '权限不足，服务器下载诊断包失败。',
    manualDownloadTip: '如自动下载失败，请检查网络联通性和浏览器安全设置，点击 ',
    manualDownload: '手动下载',
    download: '下载',
    cancel: '取消',
    details: '查看错误详情',
    retry: '重试',
    selectAll: '选择全部',
    otherErrorMsg: '生成失败，',
    closeModelTip: '您可以前往 $KYLIN-HOME/diag_dump/ 查看当前生成的诊断包。',
    closeModalTitle: '继续后诊断包将丢失，确定继续吗？',
    modelTitle: '提示',
    confrimBtn: '丢弃',
    cancelBtn: '取消',
    timeErrorMsg: '时间范围必须大于等于 5 分钟，小于等于 1 个月。请重新选择。',
    selectServerTip: '请选择服务器',
    PREPARE: '打包请求中',
    EXTRACT: '正在提取打包信息',
    COMPRESS: '正在压缩',
    DONE: '打包完成后开始下载',
    selectServerPlaceHolder: '请选择服务器',
    downloadJobDiagPackage1: '任务诊断包内容包括该任务的 Executor、Driver 日志和项目元数据等，查询报错等其他报错内容请前往',
    adminMode: '系统管理页面',
    downloadJobDiagPackage2: '下载系统诊断包。',
    downloadJobDiagPackageForNorAdmin: '任务诊断包内容包括该任务的 Executor、Driver 日志和项目元数据等，查询报错等其他报错内容请联系您的系统管理员下载系统诊断包。',
    downloadSystemDiagPackage1: '当有除任务报错之外的错误需要诊断时，请下载系统诊断包，其中包括系统元数据、系统日志。<br/>如需诊断具体任务的报错，请前往',
    jobPage: '任务页面',
    downloadSystemDiagPackage2: '下载相应的任务诊断包。',
    downloadJobDiagPackage1ForIframe: '任务诊断包内容包括该任务的 Executor、Driver 日志和项目元数据等，查询报错、集群扩缩容失败等其他报错内容请前往 ',
    workspaceList: '工作区管理页面',
    downloadJobDiagPackage2ForIframe: '下载系统诊断包。',
    downloadJobDiagPackageForNorAdminForIframe: '任务诊断包内容包括该任务的 Executor、Driver 日志和项目元数据等，查询报错等其他报错内容请联系您的系统管理员下载系统诊断包。',
    downloadJobDiagPackage3: '当前任务正在运行，下载可能会造成诊断包不完整，建议终止后下载',
    monitor: '监控页面',
    queryDiagnostic: '下载查询诊断包',
    downloadQueryDiagnostic: '查询诊断包内容包括该查询的 Executor、Driver 日志和项目元数据等，由于系统保留日志大小有限，诊断包可能不全。任务报错请前往 ',
    downloadQueryDiagnosticForKEsubText1: ' 下载任务诊断包，其他报错内容请前往 ',
    downloadQueryDiagnosticForKEsubText2: ' 下载系统诊断包。',
    downloadQueryDiagnosticForKEsubText3: ' 下载任务诊断包，其他报错内容请联系您的系统管理员下载系统诊断包。',
    downloadQueryDiagnosticForKCsubText1: ' 下载任务诊断包，集群扩缩容失败等其他报错内容请前往 ',
    deleteDiagnosticSuccess: '诊断包已丢弃',
    createDiagnostic: '生成诊断包',
    queryPage: '查询历史页面',
    downloadJobDiagnostic: '任务诊断包内容包括该任务的 Executor、Driver 日志和项目元数据等，由于系统保留日志大小有限，诊断包可能不全。查询报错请前往 ',
    downloadJobDiagnosticSubText1: ' 下载查询诊断包，其他报错内容请前往 ',
    downloadJobDiagnosticSubText2: ' 下载查询诊断包，其他报错内容请联系您的系统管理员下载系统诊断包。',
    downloadJobDiagnosticSubText3: '任务诊断包内容包括该任务的 Executor、Driver 日志和项目元数据等，由于系统保留日志大小有限，诊断包可能不全。',
    downloadJobDiagnosticSubText4: '集群扩缩容失败等其他报错内容请前往 ',
    downloadJobDiagnosticSubText5: '其他报错内容请联系您的系统管理员下载系统诊断包。'
  }
}
