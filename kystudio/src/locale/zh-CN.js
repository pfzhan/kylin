
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
    status: '状态',
    // 业务词汇
    model: '模型',
    project: '项目',
    cube: 'cube',
    models: '模型',
    cubes: 'cubes',
    dataSource: '数据源',
    fact: '事实表',
    lookup: '维度表',
    computedColumn: '计算列',
    pk: '主键',
    fk: '外键',
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
    // placeholder
    pleaseInput: '请输入',
    pleaseSelect: '请选择',
    noData: '没有数据',
    checkNoChange: '未检测到任何改动',
    // 格式提示
    nameFormatValidTip: '名称格式有误，支持数字字母下划线的组合',
    // 其他
    users: '用户',
    tip: '提示',
    action: '操作',
    help: '帮助',
    username: '用户账号',
    password: '密码'
  },
  model: {
    scanRangeSetting: '扫描范围设置',
    sameModelName: '已经存在同名的model了'
  },
  cube: {
    // for column encoding
    dicTip: '适用于大部分字段, 默认推荐使用, 但在超高基情况下, 可能引起内存不足的问题.',
    fixedLengthTip: '适用于超高基场景, 将选取字段的前N个字节作为编码值, 当N小于字段长度, 会造成字段截断, 当N较大时, 造成RowKey过长, 查询性能下降.',
    intTip: '已弃用, 请使用最新的integer编码. ',
    integerTip: '适用于字段值为整数字符, 支持的整数区间为[ -2^(8*N-1), 2^(8*N-1)] .  ',
    fixedLengthHexTip: '适用于字段值为十六进制字符, 比如1A2BFF或者FF00FF, 每两个字符需要一个字节. ',
    dataTip: '适用于字段值为日期字符, 支持的格式包括yyyyMMdd、yyyy-MM-dd、yyyy-MM-dd HH:mm:ss、yyyy-MM-dd HH:mm:ss.SSS, 其中如果包含时间戳部分会被截断. ',
    timeTip: '适用于字段值为时间戳字符, 支持范围为[ 1970-01-01 00:00:00, 2038/01/19 03:14:07] , 毫秒部分会被忽略. ',
    booleanTip: '适用于字段值为: true, false, TRUE, FALSE, True, False, t, f, T, F, yes, no, YES, NO, Yes, No, y, n, Y, N, 1, 0',
    orderedbytesTip: '',
    sameCubeName: '已经存在同名的Cube了',
    inputCubeName: '请输入cube名称',
    addCube: '添加cube'
  },
  project: {
    mustSelectProject: '请先选择一个Project',
    selectProject: '选择project'
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
    returnType: '返回类型'
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
    evaluationStatement: '您正在使用KAP试用版, 如果您对我们的产品满意, 需要专业的产品、咨询或服务, 请联系我们, 您将获得来自Apache Kylin核心小组的帮助. ',
    statement: '您已经购买KAP企业版产品及服务, 如果您在使用过程中遇到任何问题, 请随时与我们沟通, 我们将持续为您提供优质的产品及服务!'
  }
}
