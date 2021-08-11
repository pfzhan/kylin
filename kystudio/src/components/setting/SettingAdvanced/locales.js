export default {
  'en': {
    acceThreshold: 'Accelerating Notification',
    notifyLeftTips: 'Notify me whenever there are',
    notifyRightTips: 'waiting queries.',
    accePreference: 'Accelerating Preference',
    jobAlert: 'Email Notification',
    emptyDataLoad: 'Empty Data Job',
    emptyDataLoadDesc: 'Email to the following email address(es) if there is a job loading empty data.',
    errorJob: 'Error Job',
    errorJobDesc: 'Email to the following email address(es) if an error occured for the job.',
    emails: 'Email Address:',
    noData: 'No Data',
    pleaseInputEmail: 'Please enter email',
    pleaseInputVaildEmail: 'Please enter a valid email address.',
    defaultDBTitle: 'Default Database',
    defaultDB: 'Default Database',
    defaultDBNote1: 'If a database is set as default, its name could be omitted in SQL statements when executing a query or importing a SQL file.',
    defaultDBNote2: 'Modifying the default database may result in saved queries or SQL files being unavailable. Please modify it carefully.',
    confirmDefaultDBTitle: 'Modify Default Database',
    confirmDefaultDBContent: 'Modifying the default database may result in saved queries or SQL files being unavailable. Please confirm whether to modify the default database to {dbName} ?',
    yarnQueue: 'YARN Application Queue',
    yarnQueueTip: 'The system admin user can set the YARN Application Queue of the project. After setting the queue, the jobs will be submitted to the specified queue to achieve computing resources allocation and separation between projects.',
    yarnQueueWarn: 'Note: please confirm that the set queue is available, otherwise the jobs may fail to execute or be submitted to the default queue in YARN.',
    yarnIsEmpty: 'The queue name is required',
    yarnFormat: 'Incorrect format',
    computedColumns: 'Computed Columns',
    exposingCC: 'Exposing Computed Columns',
    exposingCCDesc: 'With this switch ON, computed columns would be exposed to the connected BI tools or others systems.',
    confirmCloseExposeCC: 'With this switch OFF, computed columns won\'t be exposed to the connected BI tools or other systems. It might cause the connected systems unusable. Are you sure you want to turn it off?',
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
    noEqualDecription: 'With this switch ON, you may use the history table for slowly changing dimension (as was): Non-equal join conditions (≥, <) could be used for modeling, building and queries. ',
    confirmOpen: 'Turn On',
    closeSCDTip: 'With this switch OFF, non-equal join conditions (≥, <) couldn\'t be used when editing model. The following models will go offline automatically as they include non-equal join conditions:',
    closeSCDTip1: 'To make these models online, please delete the non-equal join conditions, or turn this switch ON. Do you want to continue?',
    confirmClose: 'Turn Off',
    mulPartitionSettings: 'Multilevel Partitioning',
    mulPartition: 'Support Multilevel Partitioning',
    mulPartitionDecription: 'With this switch ON, the models under this project could be partitioned by another desired column in addition to time partitioning.',
    openMulPartitionSetting: 'Turn On Multilevel Partitioning',
    openMulPartitionTip: 'Please note that this feature is still in BETA phase. Potential risks or known limitations might exist. Check ',
    openMulPartitionTip1: ' for details.',
    closeMulPartitionSetting: 'Turn Off Multilevel Partitioning',
    closeMulPartitionTip: 'With this switch OFF,  multilevel partitioning couldn’t be used. The following models would go offline automatically as they used multilevel partitioning:',
    closeMulPartitionTip1: 'To make these models online, please delete all subpartitions, or turn this switch ON. Do you want to continue?',
    snapshotTitle: 'Snapshot Management',
    snapshotManagment: 'Support Snapshot Management',
    snapshotDesc: 'The snapshot is a read-only static view of a source table. Snapshot could reduce costs for building costs in some cases.',
    openSnapshotTitle: 'Turn On Snapshot Management',
    openManualTips: 'After turning on this option, the system will no longer automatically build, refresh, delete snapshots. You could manage them in Snapshot page under Studio.<br/>If a snapshot is being built at this time, it will show in the snapshot list after the job is completed.<br/>Do you want to continue?',
    closeSnapshotTitle: 'Turn Off Snapshot Management',
    closeManualTips: 'After turning off, you cannot manually manage snapshots. The system will build, refresh, and delete snapshots as needed. <br/>The running jobs of building or refreshing snapshot will not be affected.<br/>Do you want to continue?',
    projectConfig: 'Custom Project Configuration',
    configuration: 'Configuration',
    deleteConfig: 'Delete Configuration',
    confirmDeleteConfig: 'Are you sure you want to delete the custome configuration [{key}]?',
    secondaryStorage: 'Tiered Storage',
    supportSecStorage: 'Support Tiered Storage',
    supportSecStorageDesc: 'With this switch ON, the basic table index will be synchronized to the tiered storage. It will improve the performance of ad-hoc query and detail query analysis scenarios.',
    chooseNode: 'Please select node',
    storageNode: 'Storage Node',
    openSecStorageTitle: 'Turn On Tiered Storage',
    openSecStorageTips: 'With this switch ON, the selected nodes can\'t be removed from the project. The models under this project could use the tiered storage on demand.<br/><br/>Computed columns are not supported at the moment. The parquet files containing data prior to 1970 cannot be loaded. <a class="ky-a-like" href="https://docs.kyligence.io/books/v4.5/en/tiered_storage/" target="_blank">View the manual <i class="el-ksd-icon-spark_link_16"></i></a>',
    openSecConfirmBtn: 'Turn On',
    closeSecStorageSetting: 'Turn Off Tiered Storage',
    closeSecStorageTip: 'With this switch OFF, the data stored in the tiered storage will be cleared. The query performance might be affected.',
    affectedModels: 'The affected model:',
    secStorageInputTitle: 'Please enter "Turn Off Tiered Storage" to confirm.'
  },
  'zh-cn': {
    acceThreshold: '加速提示',
    notifyLeftTips: '每当有',
    notifyRightTips: '待加速查询时，弹出信息提醒我。',
    accePreference: '加速偏好',
    jobAlert: '邮件提醒',
    emptyDataLoad: '空数据任务',
    emptyDataLoadDesc: '当任务加载了空数据，系统会发送提醒信息至以下邮箱。',
    errorJob: '失败任务',
    errorJobDesc: '当任务报错，系统会发送提醒信息至以下邮箱。',
    emails: '邮箱：',
    noData: '暂无数据',
    pleaseInputEmail: '请输入邮箱地址',
    pleaseInputVaildEmail: '请输入有效的邮箱地址。',
    defaultDBTitle: '默认数据库',
    defaultDB: '默认数据库',
    defaultDBNote1: '设置为默认数据库后，执行查询语句时或导入 SQL 文件中可以省略该默认数据库名称。',
    defaultDBNote2: '修改默认数据库可能会导致已保存的查询或 SQL 脚本文件不可用，请谨慎修改。',
    confirmDefaultDBTitle: '修改默认数据库',
    confirmDefaultDBContent: '修改默认数据库可能会导致已保存的查询或 SQL 脚本文件不可用，请确认是否修改默认数据库为 {dbName} ？',
    yarnQueue: 'YARN 资源队列',
    yarnQueueTip: '系统管理员可以设置项目的 YARN 资源队列，设置后任务将被提交到指定的队列，以实现项目间计算资源的调配和隔离。',
    yarnQueueWarn: '注意：请确认设置的队列可用，否则任务可能会执行失败或被提交到 YARN 默认的资源队列。',
    yarnIsEmpty: '请输入队列名称',
    yarnFormat: '格式错误',
    computedColumns: '可计算列',
    exposingCC: '暴露可计算列',
    exposingCCDesc: '该选项开启后，系统会向连接的 BI 工具或其他系统暴露可计算列。',
    confirmCloseExposeCC: '若关闭该选项，可计算列将不会在连接的 BI 工具或其他系统中暴露，可能导致连接的相关系统无法使用。确定要关闭该选项吗？',
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
    nonEqualJoin: '支持基于拉链表的关联关系',
    noEqualDecription: '该选项开启后，您可以使用拉链表满足对缓慢变化维度（as was）的场景需求：可选择 ≥ 和 < 来进行建模、构建和查询。',
    confirmOpen: '确定开启',
    closeSCDTip: '关闭后，编辑模型时不再支持选择 ≥，< 的关联关系。以下模型会因包含上述条件自动下线：',
    closeSCDTip1: '若需重新上线模型，需删除模型中相应的关联关系，或将此选项开启。确认关闭吗？',
    confirmClose: '确定关闭',
    mulPartitionSettings: '多级分区',
    mulPartition: '支持多级分区',
    mulPartitionDecription: '开启多级分区后，除时间分区外，该项目下的模型可据业务场景对模型进行分区管理。',
    openMulPartitionSetting: '开启模型多级分区',
    openMulPartitionTip: '请注意，此功能尚属于 BETA 阶段，可能存在潜在风险或已知限制，',
    closeMulPartitionSetting: '关闭模型多级分区',
    closeMulPartitionTip: '关闭后，模型不再支持使用多级分区。以下模型因使用了多级分区将自动下线。',
    closeMulPartitionTip1: '若需上线需要删除子分区后才可上线或将此选项开启。确认关闭吗？',
    snapshotTitle: '快照管理',
    snapshotManagment: '支持管理快照',
    snapshotDesc: '快照（Snapshot）是原始表的只读静态视图。使用快照可以在部分场景下减少一定的构建成本。',
    openSnapshotTitle: '开启管理快照',
    openManualTips: '开启该开关后系统将不再自动构建、刷新、删除快照。您可至建模中心下的快照管理页面进行手动管理。<br/>若此时有快照正在构建，任务完成后该快照将显示在快照列表中。<br/>确认要开启吗？',
    closeSnapshotTitle: '关闭管理快照',
    closeManualTips: '关闭后您无法手动管理快照，系统将根据需要自行构建、刷新和删除快照。<br/>正在运行的构建快照和刷新快照任务将不受影响。<br/>确定关闭吗？',
    projectConfig: '自定义项目配置',
    configuration: '配置',
    deleteConfig: '删除配置',
    confirmDeleteConfig: '您确定要删除自定义配置 [{key}] 吗？',
    secondaryStorage: '分层存储',
    supportSecStorage: '支持分层存储',
    supportSecStorageDesc: '分层存储用于同步模型中的基础明细索引数据，以提高多维度灵活查询和明细查询的查询性能',
    chooseNode: '请选择节点',
    storageNode: '存储节点',
    openSecStorageTitle: '开启分层存储',
    openSecStorageTips: '开启后，该项目中使用的节点不可移除, 模型可按需使用分层存储。<br/><br/>暂时不支持可计算列，且无法加载包含1970年以前数据的 parquet 文件。<a class="ky-a-like" href="https://docs.kyligence.io/books/v4.5/zh-cn/tiered_storage/" target="_blank">查看手册<i class="el-ksd-icon-spark_link_16"></i></a>',
    openSecConfirmBtn: '确定开启',
    closeSecStorageSetting: '关闭分层存储',
    closeSecStorageTip: '关闭后，该项目中模型的分层存储数据将被清空，可能会影响查询效率。',
    affectedModels: '受影响的模型如下：',
    secStorageInputTitle: '输入“关闭分层存储”以确认操作'
  }
}
