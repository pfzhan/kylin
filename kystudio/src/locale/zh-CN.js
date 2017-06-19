
exports.default = {
  common: {
    // 常规操作
    add: '添加',
    edit: '编辑',
    delete: '删除',
    drop: '删除',
    cancel: '取消',
    close: '关闭',
    update: '更新',
    save: '保存',
    ok: '确定',
    submit: '提交',
    setting: '设置',
    logout: '注销',
    sync: '同步',
    clone: '克隆',
    check: '检测',
    view: '预览',
    draft: '草稿',
    zoomIn: '放大视图',
    zoomOut: '缩小视图',
    automaticlayout: '自动布局',
    // 常规状态
    success: '成功',
    fail: '失败',
    running: '正在采样的表',
    status: '状态',
    // 业务词汇
    model: '模型',
    project: '项目',
    projects: '项目',
    cube: 'Cube',
    models: '模型',
    jobs: '任务',
    cubes: 'Cubes',
    dataSource: '数据源',
    fact: '事实表',
    lookup: '维度表',
    limitfact: '事实表（limited）',
    computedColumn: '计算列',
    pk: '主键',
    fk: '外键',
    manual: '文档',
    tutorial: '使用教程',
    qa: '常见问题',
    // 通用提示
    unknownError: '未知错误!',
    submitSuccess: '提交成功',
    addSuccess: '添加成功',
    saveSuccess: '保存成功',
    cloneSuccess: '克隆成功',
    delSuccess: '删除成功',
    backupSuccess: '备份成功',
    updateSuccess: '更新成功',
    confirmDel: '确认删除吗？',
    checkDraft: '检测到有未保存的内容，是否继续上次的编辑？',
    // placeholder
    pleaseInput: '请输入',
    pleaseFilter: '筛选...',
    enterAlias: '输入别名',
    pleaseSelect: '请选择',
    noData: '没有数据',
    checkNoChange: '未检测到任何改动',
    // 格式提示
    nameFormatValidTip: '名称格式有误，命名支持数字、字母和下划线的组合。',
    // 其他
    users: '用户',
    tip: '提示',
    action: '操作',
    help: '帮助',
    username: '用户账号',
    password: '密码',
    saveDraft: '系统正在响应保存草稿的请求，请稍后!',
    streamingConnectHiveError: 'hive表不允许和streaming表关联'
  },
  model: {
    modelName: '模型名称：',
    modelNameGrid: '模型名称',
    modifiedGrid: '上次更新时间',
    statusGrid: '健康检测',
    metaData: '元数据',
    checkData: '采样数据',
    ownerGrid: '所有者',
    modelDesc: '描述：',
    samplingSetting: '检测设置：',
    checkModel: '模型检测',
    scanRangeSetting: '时间范围设置',
    sameModelName: '已经存在同名的模型',
    modelNameTips: '1.模型名称需要是所有项目中是唯一的。<br/> 2.模型创建后将不能重命名。',
    partitionDateTip: '1.分区日期列是可选的；如果需要经常全量构建, 请将分区日期列留空。<br/> 2.分区日期列中应该为日期值（类型可以是Date, Timestamp, String, VARCHAR, Int, Integer, BigInt）',
    partitionSplitTip: '仅用于事实表上将日期和时间分开存储于两列的情况。',
    partitionTimeTip: '分区时间列中应该为时间值（类型可以是Timestamp, String, VARCHAR）',
    modelCheckTips1: '模型健康检测将会为您检查数据中可能的问题，服务后续的cube设计和构建，以及一键优化功能。',
    modelCheckTips2: '模型检测将会触发一个检查任务（执行时间受数据量大小和采样比例影响），可以在监控界面查看该任务。',
    samplingSettingTips: '采样设置中，根据需求与集群资源情况，您可以分别设置本次模型检测的时间范围与采样比例。',
    samplingPercentage: '采样范围：',
    timeRange: '时间范围',
    samplingPercentageTips: '采样比例较高时，检测准度较高，会需要较多资源。采样比例较低时，监测准度较低，比较节省资源。',
    modelHasJob: '此模型已有在执行的模型监测任务，因此当前操作不支持。',
    viewModeLimit: '预览模式下不能进行该操作',
    modelCheck: '模型健康检测将会为您检查数据中可能的问题，服务后续的cube设计和构建，以及一键优化功能。'
  },
  cube: {
    cubeName: 'Cube 名称',
    scheduler: '构建调度器',
    schedulerTip: '使用者可以通过构建调度器设置一个增量构建的计划：第一次构建触发后，每隔固定的时间段，将新增的数据进行增量构建。',
    merge: '合并设置',
    cubeNameTip: '1.Cube名称需要是全系统唯一的。<br/>2.创建后不能修改Cube名称。',
    noticeTip: '这些状态下总是会触发一条通知。',
    dimensionTip: '1.事实表上的每个选中的列, 会自动创建为一个普通维度。<br/>2.维度表上选中的列, 可以选择定义为从外键可推导的(Derived)维度或普通维度。',
    expressionTip: 'Cube必须有一个Count(1)的度量, 并使用"_Count_"做为度量名（已经自动生成）',
    paramValueTip: '1.当参数类型是"Column"时, 只能选择一个列做为值。<br/>2.基于HyperLogLog的Distinct Count是近似计算, 请选择能接受的误差率；低误差率意味着更多的存储空间和更长的构建时间。',
    orderSumTip: '使用这个列作为TopN 的SUM和ORDER BY部分. 如果是常量(constant) 1, 将使用SUM(1)。',
    hostColumnTip: '通过Host column推导其他维度, 这里指事实表上维度之间的推导。',
    refreshSetTip: '系统会自动检查您设置的时间阈值,<br/> 例如设置了时间阈值为[ 7天, 30天] , 则系统会每隔7天合并一次segment, 并且每隔30天再合并一次已合并的segment。',
    // for column encoding
    dicTip: '适用于大部分字段，默认推荐使用。但在超高基情况下，可能引起内存不足的问题。',
    fixedLengthTip: '适用于超高基场景，将选取字段的前N个字节作为编码值。当N小于字段长度，会造成字段截断；当N较大时，造成RowKey过长，查询性能下降。',
    intTip: '已弃用，请使用最新的integer编码。',
    integerTip: '适用于字段值为整数字符，支持的整数区间为[ -2^(8*N-1), 2^(8*N-1)]。',
    fixedLengthHexTip: '适用于内容为十六进制字符的字段，比如1A2BFF或者FF00FF（每两个字符需要一个字节）。',
    dataTip: '适用于内容为日期字符的字段，支持的格式包括yyyyMMdd、yyyy-MM-dd、yyyy-MM-dd HH:mm:ss、yyyy-MM-dd HH:mm:ss.SSS（字段中包含时间戳的部分会被截断）。',
    timeTip: '适用于内容为时间戳字符的字段，支持范围为[ 1970-01-01 00:00:00, 2038/01/19 03:14:07]，毫秒部分会被忽略。',
    booleanTip: '适用于内容为：true, false； TRUE, FALSE； True, False； t, f； T, F； yes, no； YES, NO； Yes, No； y, n； Y, N； 1, 0的字段。',
    orderedbytesTip: '',
    sameCubeName: '已经存在同名的Cube',
    inputCubeName: '请输入Cube名称',
    addCube: '添加Cube',
    cubeHasJob: '此cube已有在执行的cube构建任务，因此当前操作不支持。',
    selectModelName: '请选择一个模型',
    rowkeyTip: '<h4>什么是Rowkeys?</h4><p>Rowkeys定义了维度列之间如何组织在一起</p><h4>是否按该列分散存储? </h4><p>若设为"true", Cube数据将按该列值分散存储</p><h4>Rowkey编码</h4><ol><li>"dict" 适用于大部分字段, 默认推荐使用, 但在超高基情况下, 可能引起内存不足的问题.  </li><li>"boolean" 适用于字段值为: true, false, TRUE, FALSE, True, False, t, f, T, F, yes, no, YES, NO, Yes, No, y, n, Y, N, 1, 0</li><li>"integer" 适用于字段值为整数字符, 支持的整数区间为[ -2^(8*N-1), 2^(8*N-1)] .   </li><li>"int" 已弃用, 请使用最新的integer编码.   </li><li>"date" 适用于字段值为日期字符, 支持的格式包括yyyyMMdd、yyyy-MM-dd、yyyy-MM-dd HH:mm:ss、yyyy-MM-dd HH:mm:ss.SSS, 其中如果包含时间戳部分会被截断.  </li><li>"time" 适用于字段值为时间戳字符, 支持范围为[ 1970-01-01 00:00:00, 2038/01/19 03:14:07] , 毫秒部分会被忽略.  time编码适用于time, datetime, timestamp等类型.  </li><li>"fix_length" 适用于超高基场景, 将选取字段的前N个字节作为编码值, 当N小于字段长度, 会造成字段截断, 当N较大时, 造成RowKey过长, 查询性能下降.  只适用于varchar或nvarchar类型.  </li><li>"fixed_length_hex" 适用于字段值为十六进制字符, 比如1A2BFF或者FF00FF, 每两个字符需要一个字节.  只适用于varchar或nvarchar类型.  </li></ol>'
  },
  project: {
    mustSelectProject: '请先选择一个Project',
    selectProject: '选择Project',
    projectList: '项目列表',
    addProject: '添加项目'
  },
  job: {
  },
  dataSource: {
    columnName: '列名',
    cardinality: '基数',
    dataType: '数据类型',
    comment: '注释',
    columns: '列',
    extendInfo: '扩展信息',
    statistics: '特征数据',
    sampleData: '采样数据',
    maximum: '最大值',
    minimal: '最小值',
    nullCount: '空值计数',
    minLengthVal: '最小长度值',
    maxLengthVal: '最大长度值',
    expression: '表达式',
    returnType: '数据类型',
    tableName: '表名：',
    lastModified: '修改时间：',
    totalRow: '总行数：',
    collectStatice: '采样比例越高时，收集的统计信息较准确，同时会需要较多资源。'
  },
  login: {

  },
  menu: {
    dashboard: '仪表盘',
    studio: '建模',
    insight: '分析',
    monitor: '监控',
    system: '系统',
    project: '项目'
  },
  system: {
    evaluationStatement: '您正在使用KAP试用版。如果您对我们的产品满意，需要专业的产品、咨询或服务，欢迎联系我们。您将获得来自Apache Kylin核心小组的帮助。',
    statement: '您已经购买KAP企业版产品及服务。如果您在使用过程中遇到任何问题，欢迎随时与我们沟通，我们将持续为您提供优质的产品及服务！'
  }
}
