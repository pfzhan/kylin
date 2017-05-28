
exports.default = {
  common: {
    // 常规操作
    add: '添加',
    edit: '编辑',
    delete: '删除',
    cancel: '取消',
    close: '关闭',
    update: '更新',
    save: '保存',
    submit: '提交',
    setting: '设置',
    logout: '注销',
    // 常规状态
    success: '成功',
    // 业务词汇
    model: '模型',
    cube: 'cube',
    models: '模型',
    cubes: 'cubes',
    dataSource: '数据源',
    // 通用提示
    unknownError: 'Unknown Error!',
    // 其他
    users: '用户',
    tip: '提示',
    action: '操作',
    help: '帮助'
  },
  model: {

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
    orderedbytesTip: ''
  },
  project: {

  },
  dataSource: {

  },
  login: {

  },
  menu: {
    dashboard: 'Dashboard',
    studio: 'Studio',
    insight: '分析',
    monitor: '监控',
    system: '系统',
    project: '项目'
  }
}
