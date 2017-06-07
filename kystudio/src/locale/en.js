
exports.default = {
  common: {
    // 常规操作
    add: 'add',
    edit: 'Edit',
    delete: 'delete',
    drop: 'Drop',
    cancel: 'Cancel',
    close: 'Close',
    update: 'Update',
    save: 'Save',
    ok: 'Ok',
    submit: 'Submit',
    setting: 'Settings',
    logout: 'Log Out',
    sync: 'Sync',
    clone: 'Clone',
    check: 'Check',
    view: 'View',
    draft: 'Draft',
    zoomIn: 'Zoom in',
    zoomOut: 'Zoom out',
    automaticlayout: 'Automatic layout',
    // 常规状态
    success: 'success',
    fail: 'fail',
    status: 'Status',
    // 术语
    model: 'Model',
    project: 'Project',
    cube: 'cube',
    models: 'models',
    cubes: 'cubes',
    dataSource: 'datasource',
    fact: 'Fact Table',
    lookup: 'Lookup Table',
    computedColumn: 'Computed Column',
    pk: 'Primary key',
    fk: 'Foreign key',
    // 通用提示
    unknownError: 'Unknown Error!',
    submitSuccess: 'Submitted successfully',
    addSuccess: 'Added successfully',
    saveSuccess: 'Saved successfully',
    cloneSuccess: 'Cloned successfully',
    delSuccess: 'Deleted successfully',
    backupSuccess: 'Backup successfully',
    updateSuccess: 'Updated successfully',
    confirmDel: 'Confirm delete it?',
    // placeholder
    pleaseInput: 'Please input',
    pleaseSelect: 'Please select',
    noData: 'No data',
    // 格式提示
    nameFormatValidTip: 'Invalid name！ You can use letters, numbers, and underscore characters',
    // 其他
    tip: 'Tips',
    action: 'action',
    help: 'Help',
    username: 'username',
    password: 'password'
  },
  model: {
    scanRangeSetting: 'Scan range setting',
    sameModelName: 'Model with the same name already exists'
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
    orderedbytesTip: '',
    sameCubeName: 'Cube with the same name already exists',
    inputCubeName: 'Please input cube name'
  },
  project: {
    mustSelectProject: 'Please select a project first',
    selectProject: 'Please select a project'
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
    sampleData: 'Sample Data',
    maximum: 'Max Value',
    minimal: 'Min Value',
    nullCount: 'Null Count',
    minLengthVal: 'Min Length Value',
    maxLengthVal: 'Max Length Value'
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
  },
  system: {
    evaluationStatement: 'You are using evaluation version of KAP. If you need the most professional services and products base on Apache Kylin. Please contact us! ',
    statement: 'You are using KAP enterprise product and service. If you have any issues about KAP, please contact us. We will continue to provide you with quality products and services from Apache Kylin core team.'
  }
}
