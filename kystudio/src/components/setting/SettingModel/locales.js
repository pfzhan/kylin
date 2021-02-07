export default {
  'en': {
    modifyTime: 'Last Updated Time',
    modelSetting: 'Model Settings',
    modifiedUser: 'Last Updated By',
    segmentMerge: 'Segment Merge:',
    volatileRange: 'Volatile Range:',
    retention: 'Retention Threshold:',
    newSetting: 'Add Model Setting',
    editSetting: 'Edit Model Setting',
    modelName: 'Model Name',
    settingItem: 'Setting Item',
    autoMerge: 'Auto Merge',
    volatileRangeItem: 'Volatile Range',
    retentionThreshold: 'Retention Threshold',
    HOUR: '1 Hour',
    DAY: '1 Day',
    WEEK: '1 Week',
    MONTH: '1 Month',
    QUARTER: '1 Quarter',
    YEAR: '1 Year',
    hour: 'Hour',
    day: 'Day',
    week: 'Week',
    month: 'Month',
    quarter: 'Quarter',
    year: 'Year',
    addSettingItem: 'Add Setting Item',
    isDel_auto_merge_time_ranges: 'Are you sure delete auto-merge setting item?',
    isDel_volatile_range: 'Are you sure delete volatile range setting item?',
    isDel_retention_range: 'Are you sure you want to delete the "Retention Threshold" setting?',
    'isDel_kylin.engine.spark-conf.spark.executor.cores': 'Are you sure delete spark.executor.cores item?',
    'isDel_kylin.engine.spark-conf.spark.executor.instances': 'Are you sure delete spark.executor.instances item？',
    'isDel_kylin.engine.spark-conf.spark.executor.memory': 'Are you sure delete spark.executor.memory item?',
    'isDel_kylin.engine.spark-conf.spark.sql.shuffle.partitions': 'Are you sure delete spark.sql.shuffle.partitions item?',
    'isDel_kylin.cube.aggrgroup.is-base-cuboid-always-valid': 'Are you sure delete is-base-cuboid-always-valid item?',
    autoMergeTip: 'The system could auto-merge segment fragments over different merging threshold. Auto-merge will optimize storage to enhance query performance.',
    volatileTip: '"Auto-Merge" will not merge the latest segments defined in "Volatile Range". The default value is 0.',
    retentionThresholdDesc: 'The segments within the retention threshold would be kept. The rest would be removed automatically.',
    pleaseSetAutoMerge: 'Please add \'Auto Merge\' setting first.',
    'Auto-merge': 'Auto Merge',
    'Volatile Range': 'Volatile Range',
    'Retention Threshold': 'Retention Threshold',
    'kylin.engine.spark-conf.spark.executor.cores': 'kylin.engine.spark-conf.spark.executor.cores',
    'kylin.engine.spark-conf.spark.executor.instances': 'kylin.engine.spark-conf.spark.executor.instances',
    'kylin.engine.spark-conf.spark.executor.memory': 'kylin.engine.spark-conf.spark.executor.memory',
    'kylin.engine.spark-conf.spark.sql.shuffle.partitions': 'kylin.engine.spark-conf.spark.sql.shuffle.partitions',
    'is-base-cuboid-always-valid': 'is-base-cuboid-always-valid',
    sparkCores: 'The number of cores to use on each executor.',
    sparkInstances: 'The number of executors to use on each application.',
    sparkMemory: 'The amount of memory to use per executor process.',
    sparkShuffle: 'The number of partitions to use when shuffling data for joins or aggregations.',
    baseCuboidConfig: 'According to your business scenario, you can decide whether to add an index that contains dimensions and measures defined in all aggregate groups. The index can answer queries across multiple aggregate groups, but this will impact query performance. In addition to this, there are some storage and building costs by adding this index.',
    customSettings: 'Custom Settings',
    customOptions: 'Besides the defined configurations, you can also add some advanced settings.<br/><i class="el-icon-ksd-alert"></i>Note: It\'s highly recommended to use this feature with the support of Kyligence Service Team.',
    customSettingKeyPlaceholder: 'Configuration Name',
    customSettingValuePlaceholder: 'Value',
    delCustomConfigTip: 'Are you sure you want to delete custom setting item {name}？'
  },
  'zh-cn': {
    modifyTime: '最后更新时间',
    modelSetting: '已重写的设置项',
    modifiedUser: '修改人',
    segmentMerge: 'Segment 合并：',
    volatileRange: '动态区间：',
    retention: '留存设置：',
    newSetting: '添加重写设置项',
    editSetting: '编辑重写设置项',
    modelName: '模型名称',
    settingItem: '设置项',
    autoMerge: '自动合并',
    volatileRangeItem: '动态区间',
    retentionThreshold: '留存设置',
    HOUR: '1 小时',
    DAY: '1 天',
    WEEK: '1 周',
    MONTH: '1 个月',
    QUARTER: '1 个季度',
    YEAR: '1 年',
    hour: '小时',
    day: '天',
    week: '周',
    month: '月',
    quarter: '季度',
    year: '年',
    addSettingItem: '添加重写设置项',
    isDel_auto_merge_time_ranges: '确认删除自动合并设置项吗？',
    isDel_volatile_range: '确认删除动态区间设置项吗？',
    isDel_retention_range: '确认删除留存区间设置项吗？',
    'isDel_kylin.engine.spark-conf.spark.executor.cores': '确认删除 spark.executor.cores 设置项吗？',
    'isDel_kylin.engine.spark-conf.spark.executor.instances': '确认删除 spark.executor.instances 设置项吗？',
    'isDel_kylin.engine.spark-conf.spark.executor.memory': '确认删除 spark.executor.memory 设置项吗？',
    'isDel_kylin.engine.spark-conf.spark.sql.shuffle.partitions': '确认删除 spark.sql.shuffle.partitions 设置项吗？',
    'isDel_kylin.cube.aggrgroup.is-base-cuboid-always-valid': '确认删除 is-base-cuboid-always-valid 设置项吗？',
    autoMergeTip: '根据不同层级的时间周期，系统可以自动合并 Segment 碎片。合并 Segment 就像碎片整理，可以优化查询提升查询性能。',
    volatileTip: '”自动合并“将不会合并”变动范围“内的 Segment，默认值为 0。',
    retentionThresholdDesc: '在留存阈值内的 Segment 将会被系统保留，之外的将会被自动移除。',
    pleaseSetAutoMerge: '请先添加”自动合并“设置项。',
    'Auto-merge': '自动合并',
    'Volatile Range': '动态区间',
    'Retention Threshold': '留存设置',
    'kylin.engine.spark-conf.spark.executor.cores': 'kylin.engine.spark-conf.spark.executor.cores',
    'kylin.engine.spark-conf.spark.executor.instances': 'kylin.engine.spark-conf.spark.executor.instances',
    'kylin.engine.spark-conf.spark.executor.memory': 'kylin.engine.spark-conf.spark.executor.memory',
    'kylin.engine.spark-conf.spark.sql.shuffle.partitions': 'kylin.engine.spark-conf.spark.sql.shuffle.partitions',
    'is-base-cuboid-always-valid': 'is-base-cuboid-always-valid',
    sparkCores: '单个 Executor 可用核心数。',
    sparkInstances: '单个 Application 拥有的 Executor 数量。',
    sparkMemory: '单个 Executor 最大内存。',
    sparkShuffle: '在处理 join 或 aggregation 时，shuffle 数据的分区数量。',
    baseCuboidConfig: '根据您的业务场景，可以选择是否需要添加一个包含所有聚合组内已定义的维度和度量的索引。该索引可以回答跨聚合组的查询，但是查询性能将有所下降，同时添加该索引将带来一定的存储与构建开销。',
    customSettings: '自定义设置',
    customOptions: '除了系统当前已定义的参数外，还可以通过输入一些高级参数进行配置。<br/><i class="el-icon-ksd-alert"></i>注意：我们强烈建议您在 Kyligence 服务人员的支持下使用该功能。',
    customSettingKeyPlaceholder: '请输入配置项...',
    customSettingValuePlaceholder: '请输入参数值...',
    delCustomConfigTip: '确认删除自定义参数 {name} 吗？'
  }
}
