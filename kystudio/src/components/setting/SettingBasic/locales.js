export default {
  'en': {
    basicInfo: 'General Information',
    projectName: 'Project Name:',
    projectType: 'Project Type:',
    description: 'Description:',
    AUTO_MAINTAIN: 'Smart Mode',
    MANUAL_MAINTAIN: 'AI Augmented Mode',
    pushdownSettings: 'Pushdown Setting',
    pushdownEngine: 'Pushdown Engine',
    pushdownEngineDesc: 'With pushdown engine turned on, you may query source data without indexes.',
    pushdownRange: 'Pushdown Range',
    pushdownRangeDesc: 'Turn off the pushdown range setting may cause the result difference between query on source data and indexes, especially when a source table has new data yet indexes don\'t.',
    segmentSettings: 'Segment Settings',
    segmentMerge: 'Auto-Merge',
    segmentMergeDesc: 'Segment auto-merge could help defragment the file of model index data.',
    autoMerge: 'Auto-Merge:',
    autoMergeTip: 'The system can auto-merge segment fragments over different merge threshold. Auto-merge, like defragmentation, will optimize storage to enhance query performance.',
    volatile: 'Volatile Range:',
    volatileTip: '"Auto-Merge" will not merge the latest segments defined in "Volatile Range". The default value is 0.',
    retentionThreshold: 'Retention Threshold',
    retentionThresholdDesc: 'The segments within the retention threshold would be kept. The rest would be removed automatically. For example, if the latest data time is 2019-01-01 and the retention threshold is 1 year, then the retained data is [2018-01-02, 2019-01-01].',
    minute: 'Minute',
    hour: 'Hour(s)',
    day: 'Day(s)',
    week: 'Week(s)',
    month: 'Month(s)',
    quarter: 'Quarter(s)',
    DAY1: 'daily',
    WEEK1: 'weekly',
    MONTH1: 'monthly',
    year: 'Year(s)',
    HOUR: '1 Hour',
    DAY: '1 Day',
    WEEK: '1 Week',
    MONTH: '1 Month',
    QUARTER: '1 Quarter',
    YEAR: '1 Year',
    indexOptimizationSettings: 'Index Optimization',
    storageSettings: 'Storage Setting',
    storageQuota: 'Storage Quota:',
    storageQuotaDesc: 'Project-level storage quota: If the project storage exceeds this quota, the new build index and load data job will be disabled. This could only be set by system admin.',
    storageGarbage: 'Low Usage Storage',
    storageGarbageDesc1: 'If ',
    storageGarbageDesc2: ' usage is lower than ',
    storageGarbageDesc3: ' time(s), the storage of indexes would be regarded as low usage storage.',
    storageGarbageDesc3ForSemiAutomatic: ' time(s), the storage of indexes would be regarded as low usage storage.',
    enableSemiAutomatic: 'Recommendation',
    enableSemiAutomaticDesc: 'After enabling this mode, the system will provide recommendations for existing models by analyzing the query history and model usage.',
    confirmClose: 'Turn Off',
    turnOffTips: 'After disabling this mode, the following changes will happen:<br/>* This project will not be able to <b>recommend and optimize indexes</b> and <b>create model by SQL</b>. <br/>* The existing recommendations will be <b>temporarily hidden</b> and those indexes which are <b>built successfully</b> can still serve the query.<br/>* For the recommendations, if the model definition changes, such as <b>join relationship</b> and <b>partition column</b>, all the recommendations will be <b>cleared</b> to ensure the accuracy.<br/>Do you want to continue?',
    turnOff: 'Turn Off ',
    turnOnTips: 'Please note that this feature is still in BETA phase. Potential risks or known limitations might exist. Check <a class="ky-a-like" href="https://docs.kyligence.io/books/v4.2/en/acceleration/" target="_blank">user manual</a> for details.<br/>Do you want to continue?',
    turnOn: 'Turn On ',
    confirmOpen: 'Turn On',
    queryFrequency: 'Frequency Rule',
    querySubmitter: 'User Rules',
    querySubmitterTips: 'If specified users or user groups are selected, recommendations would only be generated based on the queries from those users.',
    queryDuration: 'Duration Rule',
    AccQueryStart: 'Generate recommendations for queries used for more than ',
    AccQueryEnd: ' times',
    from: 'Generate recommendations for the queries of which the duration is between',
    to: 'to',
    secondes: 'second(s)',
    acclerationRuleSettings: 'Recommendation Settings',
    optimizationSuggestions: 'Limit of Recommendations for Adding Index',
    suggestionTip1: 'Up to',
    suggestionTip2: 'recommendations for adding indexes would be generated per time. Recommendations would be updated every day by default. The frequency could be configured. Check <a class="ky-a-like" href="https://docs.kyligence.io/books/v4.2/en/acceleration/" target="_blank">user manual</a> for details.',
    emptySegmentEnable: 'Creating Reserved Segments',
    emptySegmentEnableDesc: 'With this switch ON, you may create a segment with no index. Please note that queries would be answered by the pushdown engine (if turned on) when they hit empty segments.',
    overTimeLimitTip: 'The upper limit of the duration range can\'t exceed 3600 seconds',
    prevGreaterThanNext: 'The upper limit of the duration range should be greater than the lower limit'
  },
  'zh-cn': {
    basicInfo: '通用信息',
    projectName: '项目名称：',
    projectType: '项目类型：',
    description: '描述：',
    AUTO_MAINTAIN: '智能模式',
    MANUAL_MAINTAIN: 'AI 增强模式',
    pushdownSettings: '查询下压',
    pushdownEngine: '查询下压引擎',
    pushdownEngineDesc: '开启查询下压引擎后，将可以在没有索引时分析数据。',
    pushdownRange: '查询下压的范围',
    pushdownRangeDesc: '关闭对查询下压范围的检查后，如果源数据已经有新数据，可能导致在源数据中查询到的数据量比查询索引时大。',
    segmentSettings: 'Segment 设置',
    segmentMerge: '自动合并',
    segmentMergeDesc: 'Segment 自动合并能够减少模型索引数据中的碎片。',
    autoMerge: '自动合并：',
    autoMergeTip: '根据不同层级的时间周期，系统可以自动合并 segment 碎片。合并 segment 就像碎片整理，可以优化查询提升查询性能。',
    volatile: '变动范围：',
    volatileTip: '“自动合并”将不会合并“变动范围”内的 Segment，默认值为 0。',
    retentionThreshold: '留存阈值',
    retentionThresholdDesc: '在留存阈值内的 Segment 将会被系统保留，之外的将会被自动移除。例如，最新数据时间为 2020-01-01，留存阈值为 1 年，则早于 2019-01-02 的 Segment 将被移除。',
    minute: '分钟',
    hour: '小时',
    day: '天',
    week: '周',
    month: '月',
    quarter: '季度',
    year: '年',
    DAY1: '每天',
    WEEK1: '每周',
    MONTH1: '每月',
    HOUR: '1 小时',
    DAY: '1 天',
    WEEK: '1 周',
    MONTH: '1 月',
    QUARTER: '1 季度',
    YEAR: '1 年',
    indexOptimizationSettings: '索引优化',
    storageSettings: '存储设置',
    storageQuota: '存储配额：',
    storageQuotaDesc: '项目级存储配额：如果项目存储超过该配额，新的构建索引和加载数据任务将被禁止。仅系统管理员可以设置。',
    storageGarbage: '低效存储',
    storageGarbageDesc1: '当',
    storageGarbageDesc2: '使用频率低于',
    storageGarbageDesc3: '次的索引查询所对应的存储即为低效存储。',
    storageGarbageDesc3ForSemiAutomatic: '次的索引查询所对应的存储即为低效存储。',
    enableSemiAutomatic: '智能推荐',
    enableSemiAutomaticDesc: '开启智能推荐模式后，系统将根据您的查询历史及使用情况，对已有模型提供优化建议。',
    confirmClose: '确认关闭',
    turnOffTips: '关闭智能推荐后:<br/>* 当前项目将不具备<b>任何索引推荐和优化能力</b>，以及<b>SQL建模</b>等。<br/>* 当前已存在的建议将<b>暂时隐藏</b>，已<b>构建完成</b>的索引仍可以服务于查询。<br/>* 对于优化建议，当模型的定义如<b>关联关系</b>，<b>分区列</b>等发生变化时，为了保证优化建议的准确性此时会<b>清空</b>掉所有的优化建议。<br/>确认关闭吗？',
    turnOff: '关闭',
    turnOnTips: '请注意，此功能尚属于 BETA 阶段，可能存在潜在风险或已知限制。详情请<a class="ky-a-like" href="https://docs.kyligence.io/books/v4.2/zh-cn/acceleration/" target="_blank">查看手册</a><br/>确认要开启吗？',
    turnOn: '开启',
    confirmOpen: '确认开启',
    queryFrequency: '查询频率',
    querySubmitter: '用户规则',
    querySubmitterTips: '如果选择了指定用户或用户组，将只会根据这些用户的查询生成优化建议。',
    queryDuration: '查询耗时',
    AccQueryStart: '对使用次数大于',
    AccQueryEnd: '的查询生成优化建议。',
    from: '对耗时的范围在',
    to: '秒到',
    secondes: '秒的查询生成优化建议。',
    acclerationRuleSettings: '优化建议规则设置',
    optimizationSuggestions: '优化建议数量上限',
    suggestionTip1: '每次最多生成',
    suggestionTip2: '条类型为新增索引的优化建议。默认每天更新一次，此更新频率可进行配置，详情请<a class="ky-a-like" href="https://docs.kyligence.io/books/v4.2/zh-cn/acceleration/" target="_blank">查看手册</a>。',
    emptySegmentEnable: '支持创建保留 Segment',
    emptySegmentEnableDesc: '该选项开启后，您可以在 Segment 列表页面直接创建一个不包含任何索引的 Segment。请注意，查询命中空的 Segment 时，如果同时开启了查询下压引擎，将会通过下压查询回答。',
    overTimeLimitTip: '耗时范围的上限不超过 3600 秒',
    prevGreaterThanNext: '耗时范围的上限应大于下限'
  }
}
