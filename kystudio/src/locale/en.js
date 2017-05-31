
exports.default = {
  common: {
    // 常规操作
    add: 'add',
    edit: 'Edit',
    delete: 'delete',
    drop: 'Drop',
    cancel: 'cancel',
    close: 'close',
    update: 'update',
    save: 'save',
    submit: 'submit',
    setting: 'Settings',
    logout: 'Logout',
    sync: 'Sync',
    clone: 'Clone',
    check: 'Check',
    // 常规状态
    success: 'success',
    fail: 'fail',
    status: 'Status',
    // 术语
    model: 'model',
    cube: 'cube',
    models: 'models',
    cubes: 'cubes',
    dataSource: 'datasource',
    // 通用提示
    unknownError: 'Unknown Error!',
    addSuccess: 'Added successfully',
    saveSuccess: 'Saved successfully',
    delSuccess: 'Deleted successfully',
    updateSuccess: 'Updated successfully',
    // 其他
    tip: 'Tips',
    action: 'action',
    help: 'help'
  },
  model: {

  },
  cube: {
    // for column encoding
    dicTip: 'Use dictionary to encode dimension values. dict encoding is very compact but vulnerable for ultra high cardinality dimensions. ',
    fixedLengthTip: 'Use a fixed-length("length" parameter) byte array to encode integer dimension values, with potention value truncations. ',
    intTip: 'Deprecated, use latest integer encoding intead. ',
    integerTip: 'Use N bytes to encode integer values, where N equals the length parameter and ranges from 1 to 8. [ -2^(8*N-1), 2^(8*N-1)) is supported for integer encoding with length of N. ',
    fixedLengthHexTip: 'Use a fixed-length("length" parameter) byte array to encode the hex string dimension values, like 1A2BFF or FF00FF, with potention value truncations. Assign one length parameter for every two hex codes. ',
    dataTip: 'Use 3 bytes to encode date dimension values. ',
    timeTip: 'Use 4 bytes to encode timestamps, supporting from 1970-01-01 00:00:00 to 2038/01/19 03:14:07. Millisecond is ignored. ',
    booleanTip: 'Use 1 byte to encode boolean values, valid value include: true, false, TRUE, FALSE, True, False, t, f, T, F, yes, no, YES, NO, Yes, No, y, n, Y, N, 1, 0',
    orderedbytesTip: ''
  },
  project: {
    mustSelectProject: 'Please select a project first'
  },
  job: {
  },
  dataSource: {
    columnName: 'Column Name',
    cardinality: 'Cardinality',
    dataType: 'Data Type',
    comment: 'Comment',
    columns: 'Columns',
    extendInfo: 'Extend Information',
    statistics: 'Statistics',
    sampleData: 'Sample Data'
  },
  login: {

  },
  menu: {
    dashboard: 'Dashboard',
    studio: 'Studio',
    insight: 'Insight',
    monitor: 'Monitor',
    system: 'System',
    project: 'Project'
  }
}
