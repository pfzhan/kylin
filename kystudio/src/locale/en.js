
exports.default = {
  common: {
    // 常规操作
    add: 'Add',
    edit: 'Edit',
    delete: 'Delete',
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
    detail: 'Detail',
    draft: 'Draft',
    zoomIn: 'Zoom In',
    zoomOut: 'Zoom Out',
    automaticlayout: 'Automatic Layout',
    // 常规状态
    success: 'Success',
    fail: 'Fail',
    status: 'Status',
    // 术语
    model: 'Model',
    project: 'Project',
    projects: 'Projects',
    cube: 'cube',
    models: 'Models',
    jobs: 'Jobs',
    cubes: 'Cubes',
    dataSource: 'Datasource',
    fact: 'Fact Table',
    limitfact: 'Fact Table(limited)',
    lookup: 'Lookup Table',
    computedColumn: 'Computed Column',
    pk: 'Primary key',
    fk: 'Foreign key',
    manual: 'Documentation',
    tutorial: 'Tutorial',
    qa: 'FAQ',
    // 通用提示
    unknownError: 'Unknown Error.',
    submitSuccess: 'Submitted successfully',
    addSuccess: 'Added successfully',
    saveSuccess: 'Saved successfully',
    cloneSuccess: 'Cloned successfully',
    delSuccess: 'Deleted successfully',
    backupSuccess: 'Back up successfully',
    updateSuccess: 'Updated successfully',
    confirmDel: 'Confirm delete it?',
    checkDraft: 'Detected the unsaved content, are you going to continue the last edit?',
    // placeholder
    pleaseInput: 'Please input here',
    pleaseFilter: 'filter...',
    enterAlias: 'enter alias',
    pleaseSelect: 'Please select',
    noData: 'No data',
    checkNoChange: 'No content changed',
    // 格式提示
    nameFormatValidTip: 'Invalid name! Only letters, numbers and underscore characters are supported in a valid name.',
    // 其他
    users: 'Users',
    tip: 'Tips',
    action: 'Action',
    help: 'Help',
    username: 'username',
    password: 'password'
  },
  model: {
    samplingSetting: 'Sampling Setting:',
    checkModel: 'Check Model',
    scanRangeSetting: 'Time range setting',
    sameModelName: 'Model with the same name existed',
    modelCheckTips1: 'Model health check is highly recommended to help generate a cube optimize result.',
    modelCheckTips2: 'Trigger model check will send a job (executing time depends ondataset scale and sampling setting), you can view it on monitor page.',
    samplingSettingTips: 'Here you can set time range and sampling ratio based on your demand and cluster resource.',
    samplingPercentage: 'Sampling percentage:',
    timeRange: 'Time range',
    samplingPercentageTips: 'If sampling ratio is high, check job would return accurate results <br/> with high resource engaged. If sampling ratio is low, check job would <br/>return less accurate results with resource saving.'
  },
  cube: {
    // for column encoding
    dicTip: 'Dict encoding applies to most columns and is recommended by default. But in the case of ultra-high cardinality, it may cause the problem of insufficient memory.',
    fixedLengthTip: 'Fixed-length encoding applies to the ultra-high cardinality scene, and will select the first N bytes of the column as the encoded value. When N is less than the length of the column, it will cause the column to be truncated; when N is large, the Rowkey is too long and the query performance is degraded.',
    intTip: 'Deprecated, please use integer encoding instead.',
    integerTip: 'For integer characters, the supported integer range is [-2 ^ (8 * N-1), 2 ^ (8 * N-1)].',
    fixedLengthHexTip: 'Use a fixed-length("length" parameter) byte array to encode the hex string dimension value, supporting formats include yyyyMMdd, yyyy-MM-dd, yyyy-MM-dd HH: mm: ss, yyyy-MM-dd HH: mm: ss.SSS (the section containing the timestamp in the column will be truncated).',
    dataTip: 'Use 3 bytes to encode date dimension value.',
    timeTip: 'Use 4 bytes to encode timestamp, supporting from 1970-01-01 00:00:00 to 2038/01/19 03:14:07. Millisecond is ignored. ',
    booleanTip: 'Use 1 byte to encode boolean value, valid value including: true, false; TRUE, FALSE; True, False; t, f; T, F; yes, no; YES, NO; Yes, No; y, n, Y, N, 1, 0.',
    orderedbytesTip: '',
    sameCubeName: 'Cube with the same name existed',
    inputCubeName: 'Please input a cube name',
    addCube: 'Add Cube'
  },
  project: {
    mustSelectProject: 'Please select a project first',
    selectProject: 'Please select a project',
    projectList: 'Project List',
    addProject: 'Add Project'
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
    maxLengthVal: 'Max Length Value',
    expression: 'Expression',
    returnType: 'Data Type',
    tableName: 'Table Name:',
    lastModified: 'Last Modified:',
    totalRow: 'Total Rows:',
    collectStatice: 'The higher the sampling percentage, the more accurate the stats information, the more resources engaging.'
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
    evaluationStatement: 'You are using KAP with Evaluation License. For more product information, expert consulting and services, please contact us. We’ll get you the help you need from Apache Kylin core team.',
    statement: 'You have purchased KAP with Enterprise License and services. If you encounter any problems in the course of use, please feel free to contact us. We will provide you with consistent quality products and services.'
  }
}
