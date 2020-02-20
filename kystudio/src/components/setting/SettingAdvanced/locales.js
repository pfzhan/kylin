export default {
  'en': {
    acceThreshold: 'Accelerating Notification',
    notifyLeftTips: 'Notify me whenever there are',
    notifyRightTips: 'waiting queries.',
    accePreference: 'Accelerating Preference',
    jobAlert: 'Job Notification',
    emptyDataLoad: 'Empty Data Load',
    emptyDataLoadDesc: 'Email to following email address if there is an empty data load job.',
    errorJob: 'Error Job',
    errorJobDesc: 'Email to following email address if there is an error job.',
    emails: 'Emails:',
    noData: 'No Data',
    pleaseInputEmail: 'Please input email',
    pleaseInputVaildEmail: 'Please input vaild email.',
    defaultDBTitle: 'Default Database',
    defaultDB: 'Default Database',
    defaultDBNote1: 'After setting the default database, the database name can be omitted in SQL statements when executing a query or importing a SQL file.',
    defaultDBNote2: 'Note: modifying the default database may result in saved queries or SQL files being unavailable. Please modify the default database prudently. ',
    confirmDefaultDBTitle: 'Modify Default Database',
    confirmDefaultDBContent: 'Modifying the default database may result in saved queries or SQL files being unavailable. Please confirm whether to modify the default database to {dbName} ?',
    yarnQueue: 'YARN Application Queue',
    yarnQueueTip: 'The system admin user can set the YARN Application Queue of the project. After setting the queue, the jobs will be submitted to the specified queue to achieve computing resources allocation and separation between projects.',
    yarnQueueWarn: 'Note: please confirm that the set queue is available, otherwise the jobs may fail to execute or be submitted to the default queue in YARN.',
    yarnIsEmpty: 'The queue name is required',
    yarnFormat: 'Incorrect format',
    computedColumns: 'Computed Columns',
    exposingCC: 'Exposing Computed Columns',
    exposingCCDesc: 'After turning on this option, when the system is connected to Kyligence Enterprise, the system will expose computed columns to it.',
    confirmCloseExposeCC: 'If you turn off this option, when the system is connected to Kyligence Enterprise, Kyligence Enterprise will not expose the computed columns defined in Kyligence Enterprise to it. This operation may make the system connected to Kyligence Enterprise unusable.'
  },
  'zh-cn': {
    acceThreshold: '加速提示',
    notifyLeftTips: '每当有',
    notifyRightTips: '待加速查询时，弹出信息提醒我。',
    accePreference: '加速偏好',
    jobAlert: '任务提醒',
    emptyDataLoad: '空数据任务',
    emptyDataLoadDesc: '如果有任务加载了空数据，请发邮件给以下邮箱提醒。',
    errorJob: '失败任务',
    errorJobDesc: '如果有报错任务，请发邮件给以下邮箱提醒。',
    emails: '邮箱：',
    noData: '暂无数据',
    pleaseInputEmail: '请输入邮箱地址',
    pleaseInputVaildEmail: '请输入正确的邮箱地址。',
    defaultDBTitle: '默认数据库',
    defaultDB: '默认数据库',
    defaultDBNote1: '设置为默认数据库后，执行查询语句时或导入 SQL 文件中可以省略数据库名称。',
    defaultDBNote2: '注意：修改默认数据库可能会导致已保存的查询或 SQL 脚本文件不可用，请您谨慎修改默认数据库。',
    confirmDefaultDBTitle: '修改默认数据库',
    confirmDefaultDBContent: '修改默认数据库可能会导致已保存的查询或 SQL 脚本文件不可用，请确认是否修改默认数据库为 {dbName} ？',
    yarnQueue: 'YARN 资源队列',
    yarnQueueTip: '系统管理员可以设置项目的 YARN 资源队列，设置后任务将被提交到指定的队列，以实现项目间计算资源的调配和隔离。',
    yarnQueueWarn: '注意：请确认设置的队列可用，否则任务可能会执行失败或被提交到 YARN 默认的资源队列。',
    yarnIsEmpty: '请输入队列名称',
    yarnFormat: '格式错误',
    computedColumns: '可计算列',
    exposingCC: '暴露可计算列',
    exposingCCDesc: '开启该选项后，当系统对接 Kyligence Enterprise 时，系统会向其暴露可计算列。',
    confirmCloseExposeCC: '若关闭该选项，当系统对接 Kyligence Enterprise 时，Kyligence Enterprise 将不会向其暴露定义在 Kyligence Enterprise 中的可计算列。该操作可能会导致此前对接 Kyligence Enterprise 的系统无法使用。'
  }
}
