export default {
  'en': {
    adminTips: 'Admin user can view all job information via Select All option in the project list.',
    streamingJobsList: 'Streaming Job List',
    jobStart: 'Start',
    jobRestart: 'Restart',
    jobStop: 'Stop',
    refreshList: 'Refresh List',
    pleaseSearch: 'Search Object',
    streamingIngestion: 'Data Ingestion',
    autoMerge: 'Auto-Merge',
    TargetObject: 'Object',
    JobType: 'Type',
    project: 'Project',
    status: 'Status',
    dataDuration: 'Data Latency',
    lastStatusChangeTime: 'Last Status Change Time',
    lastStatusDuration: 'Last Status Duration',
    Actions: 'Actions',
    configurations: 'Configurations',
    START: 'START',
    STARTING: 'STARTING',
    RUNNING: 'RUNNING',
    STOPPING: 'STOPPING',
    STOPPED: 'STOPPED',
    STOP: 'STOP',
    ERROR: 'ERROR',
    clearAll: 'Clear All',
    filteredTotalSize: '{totalSize} result(s)',
    ProgressStatus: 'Status',
    key: 'Key',
    value: 'Value',
    STREAMING_BUILD: 'STREAMING_BUILD',
    STREAMING_MERGE: 'STREAMING_MERGE',
    pleaseInputKey: 'Please enter the key',
    pleaseInputValue: 'Please enter the value',
    dataStatistics: 'Data Statistics',
    jobRecords: 'Job Start and Stop Records',
    noSelectJobs: 'Please select at least one job.',
    recordStatus: 'Status Change Records',
    consumptionRate: 'Consumption Rate',
    dataLatency: 'Data Latency',
    processingTime: 'Processing Time',
    consumptionUnit: 'msg/s',
    latencyUnit: 's',
    processingUnit: 's',
    isNoStatisticsData: 'No Statistics',
    isNoRecords: 'No Records',
    '1d': '1 d',
    '3d': '3 d',
    '7d': '7 d',
    stopStreamingJobTips: 'Stopping the job(s) may take several minutes. Are you sure you want to stop the job(s)?',
    stopStreamingJobImmeTips: 'Stopping the job immediately, some segments would be in "REFRESHING" or "LOCKED" state. They would be deleted when the job(s) starts again, or you could delete them manually. Are you sure you want to stop the job(s) immediately?',
    stopJob: 'Stop Job',
    stopJobImme: 'Stop Immediately',
    mulParamsKeyTips: 'This key already exists.',
    errorStautsTips: 'An unknown error occurred. Please contact the admin.',
    errorStautsTips2: 'An unknown error occurred. Please download system diagnostic package to view the log or contact the admin.',
    disableStartJobTips: 'Can\'t be used. Please add streaming indexes first.',
    borkenModelDisableStartJobTips: 'Can\'t be used. Model {modelName} is currently broken.',
    logInfoTip: 'Log Output',
    output: 'Output'
  },
  'zh-cn': {
    adminTips: '系统管理员可以在项目列表中选择全部项目，查看所有项目下的任务信息。',
    streamingJobsList: '流数据任务列表',
    jobStart: '启动',
    jobRestart: '重启',
    jobStop: '停止',
    refreshList: '刷新列表',
    pleaseSearch: '搜索任务对象',
    streamingIngestion: '数据摄入',
    autoMerge: '自动合并',
    TargetObject: '对象',
    JobType: '类型',
    project: '项目',
    status: '状态',
    dataDuration: '数据延迟',
    lastStatusChangeTime: '最近状态变更时间',
    lastStatusDuration: '最近状态持续时间',
    Actions: '操作',
    configurations: '参数配置',
    START: '启动',
    STARTING: '启动中',
    RUNNING: '运行',
    STOPPING: '停止中',
    STOPPED: '停止',
    ERROR: '错误',
    STOP: '停止',
    clearAll: '清除所有',
    filteredTotalSize: '{totalSize} 条结果',
    ProgressStatus: '状态',
    key: '参数',
    value: '参数值',
    STREAMING_BUILD: '数据摄入',
    STREAMING_MERGE: '自动合并',
    pleaseInputKey: '请输入参数名称',
    pleaseInputValue: '请输入参数值',
    dataStatistics: '数据统计',
    jobRecords: '任务启停记录',
    noSelectJobs: '请选择至少一个任务。',
    recordStatus: '状态变更记录',
    consumptionRate: '消费速率',
    dataLatency: '数据延迟',
    processingTime: '处理时长',
    consumptionUnit: 'msg/s',
    latencyUnit: 's',
    processingUnit: 's',
    isNoStatisticsData: '无统计信息',
    isNoRecords: '无记录',
    '1d': '1 天',
    '3d': '3 天',
    '7d': '7 天',
    stopStreamingJobTips: '停止任务可能需要花费几分钟，确定要停止任务吗？',
    stopStreamingJobImmeTips: '立即停止，可能导致一些 Segment 处于“REFRESHING”或“LOCKED”状态。这些 Segment 将在任务下次启动时自动删除，也可被手动删除。确定要立即停止任务吗？',
    stopJob: '停止任务',
    stopJobImme: '立即停止',
    mulParamsKeyTips: '该参数已存在',
    errorStautsTips: '出现未知错误，请联系管理员查看。',
    errorStautsTips2: '出现未知错误，请下载系统诊断包查看日志或联系管理员。',
    disableStartJobTips: '无法使用，请先添加流数据索引',
    borkenModelDisableStartJobTips: '无法使用，模型 {modelName} 当前为 Broken 状态。',
    logInfoTip: '日志详情',
    output: '输出'
  }
}
