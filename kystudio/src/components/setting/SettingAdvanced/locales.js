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
    exposingCCDesc: 'After turning on this option, when the BI or others system are connected to Kyligence Enterprise, the system will expose computed columns to it.',
    confirmCloseExposeCC: 'If you turn off this option, when the BI or others system are connected to Kyligence Enterprise, Kyligence Enterprise will not expose the computed columns defined in Kyligence Enterprise to it. This operation may make the system connected to Kyligence Enterprise unusable.',
    kerberosAcc: 'Kerberos Configuration',
    principalName: 'Principal Name',
    keytabFile: 'Keytab File',
    kerberosTips: 'System admin can configure the Kerberos account in project level. After that, Hive table list will be shown with the permission of this Kerberos permission.',
    maxSizeLimit: 'Files cannot exceed 5M.',
    fileTypeError: 'Invalid file format.',
    uploadFileTips: 'Supported file formats is keytab. Supported file size is up to 5 MB.',
    selectFile: 'Select the file',
    kerberosFileRequied: 'Please upload keytab file',
    refreshTitle: 'Refresh DataSource',
    refreshNow: 'Refresh Now',
    refreshLater: 'Refresh Later',
    refreshContent: 'Update successfully. The configuration will take effect after refreshing the datasource cache. Please confirm whether to refresh now. </br> Note: It will take a long time to refresh the cache. If you need to configure multiple projects, it is recommended to refresh when configuring the last project.',
    openSCDSetting: 'Turn On Support History table',
    openSCDTip: 'Please note that this feature is still in BETA phase. Potential risks or known limitations might exist. Check ',
    userManual: 'user manual',
    openSCDTip1: ' for details.',
    closeSCDSetting: 'Turn Off Support History table',
    confirmOpenTip: 'Do you want to continue?',
    SCD2Settings: 'Support History table',
    nonEqualJoin: 'Show non-equal join conditions for History table',
    noEqualDecription: 'With this switch ON, you may use History table for slowly changing dimension (as was): Non-equal join conditions (≥，<) could be used for modeling, building and queries. ',
    confirmOpen: 'Turn On',
    closeSCDTip: 'With this switch OFF, non-equal join conditions (≥, <) couldn\'t be used when editing model. The following models will go offline automatically as they include non-equal join conditions:',
    closeSCDTip1: 'To make these models online, please delete the non-equal join conditions, or turn this switch ON. Do you want to continue?',
    confirmClose: 'Turn Off'
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
    exposingCCDesc: '开启该选项后，当BI或其他系统对接 Kyligence Enterprise 时，系统会向其暴露可计算列。',
    confirmCloseExposeCC: '若关闭该选项，当BI或其他系统对接 Kyligence Enterprise 时，Kyligence Enterprise 将不会向其暴露定义在 Kyligence Enterprise 中的可计算列。该操作可能会导致此前对接 Kyligence Enterprise 的系统无法使用。',
    kerberosAcc: 'Kerberos 配置',
    principalName: 'Principal 名称',
    keytabFile: 'Keytab 文件',
    kerberosTips: '系统管理员可以对项目设置独立的 Kerberos 账户信息。配置后，当前项目下将按照对应的账户信息展示 Hive 列表。',
    maxSizeLimit: '文件大小不能超过 5M!',
    fileTypeError: '不支持的文件格式！',
    uploadFileTips: '支持的文件格式为 keytab，文件最大支持 5 MB。',
    selectFile: '选择文件',
    kerberosFileRequied: '请上传 keytab 文件',
    refreshTitle: '刷新数据源',
    refreshNow: '立即刷新',
    refreshLater: '稍后刷新',
    refreshContent: '更新成功。刷新数据源缓存后配置生效。请问是否刷新数据源缓存？</br> 提示：刷新缓存时间较长，如您需要配置多个项目，建议配置最后一个项目时进行刷新。',
    openSCDSetting: '开启支持拉链表',
    closeSCDSetting: '关闭支持拉链表',
    openSCDTip: '请注意，此功能尚属于 BETA 阶段，可能存在潜在风险或已知限制。详情请',
    userManual: '查看手册',
    confirmOpenTip: '确认要开启吗？',
    SCD2Settings: '支持拉链表',
    nonEqualJoin: '支持基于拉链表的连接条件',
    noEqualDecription: '该选项开启后，您可以使用拉链表满足对缓慢变化维度（as was）的场景需求：可选择 ≥ 和 < 来进行建模、构建和查询。',
    confirmOpen: '确定开启',
    closeSCDTip: '关闭后，编辑模型时不再支持选择 ≥，< 的连接条件。以下模型会因包含上述条件自动下线：',
    closeSCDTip1: '若需重新上线模型，需删除模型中相应的连接条件，或将此选项开启。确认关闭吗？',
    confirmClose: '确定关闭'
  }
}
