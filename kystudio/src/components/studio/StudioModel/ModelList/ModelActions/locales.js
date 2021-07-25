export default {
  en: {
    authorityDetails: 'The details of authority',
    iKnow: 'Got it',
    exportTds: 'Export TDS',
    rename: 'Rename',
    delete: 'Delete',
    purge: 'Purge',
    disable: 'Disable',
    enable: 'Enable',
    onLine: 'Online',
    offLine: 'Offline',
    build: 'Build Index',
    buildTitle: 'Build',
    modelName: 'Model',
    changeModelOwner: 'Change Owner',
    modelPartitionSet: 'Model Partition',
    recommendations: 'Recommendation',
    subPartitionValuesManage: 'Manage Sub-Partition Values',
    secStorage: 'Tiered Storage',
    openSecStorageTips: 'The system will create a base table index for model "{modelName}". The index can\'t be deleted when the tiered storage is ON.',
    openSecStorageTips2: 'With the tiered storage ON, the existing data needs to be loaded to tiered storage to take effect. ',
    closeSecStorageTips: 'With this switch OFF, the model\'s tiered storage data will be cleared。',
    secStorageDesc: 'With this switch ON, the basic table index will be synchronized to the tiered storage. It will improve the performance of ad-hoc query and detail query analysis scenarios.',
    disableSecStorageActionTips2: 'The tiered storage can\'t be used because no dimension or measure has been added and the base table index can\'t be added.',
    supportSecStoage: 'Support Tiered Storage',
    node: 'Node',
    exportMetadatas: 'Export Model',
    bokenModelExportTDSTip: 'Can\'t export TDS file at the moment as the model is BROKEN',
    bokenModelExportMetadatasTip: 'Can\'t export model file at the moment as the model is BROKEN',
    buildTips: 'The indexes in this model have not been built and are not ready for serving queries. Please build indexes to optimize query performance.',
    multilParTip: 'This model used multilevel partitioning, which are not supported at the moment. Please set subpartition as \'None\' in model partition dialog, or turn on \'Multilevel Partitioning\' in project settings.',
    change: 'Change',
    changeTo: 'Change Owner To',
    pleaseChangeOwner: 'Please change model owner',
    changeDesc: 'You can change the owner of the model to a system admin, a user in the project ADMIN role, or a user in the project management role.',
    exportTDSContinueBtn: 'Got it',
    exportTDSTitle: 'Export TDS',
    step1: 'Select included columns for dimension and measure',
    step2: 'Select how to connect data source',
    exportTDSOptions1: 'Only include the ones included in aggregate indexes（default）',
    exportTDSOptions2: 'Include the ones in both aggregate indexes and table indexes',
    exportTDSOptions3: 'Include all columns for dimension or measure, even if they are not included in any aggregate or table index',
    connectODBC: 'Other ODBC data sources（default）',
    connectTableau: 'Tableau Kyligence Connector',
    exportTDSOfflineTips: 'The exported TDS file can\'t be used for queries if the model is OFFLINE. Please make sure that the model goes online when using this TDS file.',
    delModelTip: 'Are you sure you want to drop the model {modelName}?',
    delModelTitle: 'Delete Model',
    pergeModelTip: 'Are you sure you want to purge all segments of the model {modelName}?',
    pergeModelTitle: 'Purge Model',
    disableModelSuccessTip: 'Offline the model successfully.',
    enabledModelSuccessTip: 'Online the model successfully.',
    purgeModelSuccessTip: 'Successfully purged the model',
    deleteModelSuccessTip: 'Delete the model successfully.',
    noSegmentOnlineTip: 'This model can\'t go online as it doesn\'t have segments. Models with no segment couldn\'t serve queries. Please add a segment.',
    cannotOnlineTips: 'This model can\'t go online at the moment:',
    disableModelTip: 'The offline model couldn\'t serve queries. The built indexes could still be used after the model is online again. Are you sure you want to offline model "{modelName}"?',
    disableModelTitle: 'Offline Model',
    noIndexTips: 'You should add indexes first before building.',
    changeModelSuccess: 'The owner of model {modelName} has been successfully changed to {owner}.',
    jobSuccess: 'Submitted successfully. You may go to  the job page to ',
    disableActionTips: 'Unavailable for Streaming model',
    disableActionTips2: 'Unavailable for Fusion model and Streaming model',
    disableActionTips3: 'Unavailable for Fusion model',
    disableActionTips4: 'The time partition settings can\'t be modified for fusion model and streaming model. ',
    disableSecStorageActionTips: 'The tiered storage can\'t be used for fusion or streaming models at the moment.'
  },
  'zh-cn': {
    authorityDetails: '权限详情',
    iKnow: '知道了',
    exportTds: '导出 TDS',
    rename: '重命名',
    delete: '删除',
    purge: '清空',
    disable: '禁用',
    enable: '启用',
    onLine: '模型上线',
    offLine: '模型下线',
    build: '构建索引',
    buildTitle: '构建',
    modelName: '模型',
    changeModelOwner: '变更所有者',
    modelPartitionSet: '分区设置',
    recommendations: '优化建议',
    subPartitionValuesManage: '子分区值设置',
    secStorage: '分层存储',
    openSecStorageTips: '开启后系统将为模型“{modelName}”创建一个基础明细索引。在开启分层存储时该索引不可删除。',
    openSecStorageTips2: '开启分层存储后，现有 Segment 数据需加载到分层存储后生效。',
    disableSecStorageActionTips2: '模型未定义维度/度量，无法生成基础明细索引和使用分层缓存。',
    closeSecStorageTips: '关闭后，模型的分层存储数据将被清空，可能会影响查询效率。',
    secStorageDesc: '分层存储用于同步模型中的基础明细索引数据，以提高多维度灵活查询和明细查询的查询性能。',
    supportSecStoage: '支持分层存储',
    node: '节点',
    exportMetadatas: '导出模型',
    bokenModelExportTDSTip: '该模型状态为 BROKEN，无法导出 TDS 文件',
    bokenModelExportMetadatasTip: '该模型状态为 BROKEN，无法导出',
    buildTips: '模型尚未构建索引，不可服务于查询分析。构建索引后可优化查询性能。',
    multilParTip: '该模型使用了多级分区，当前不可用。请在分区设置中将子分区列设为”无分区”，或在项目设置中开启支持多级分区。',
    change: '变更',
    changeTo: '变更所有者为',
    pleaseChangeOwner: '请选择模型所有者',
    changeDesc: '您可以将该模型的所有者变更为系统管理员、项目 ADMIN 角色的用户或者项目 Management 角色的用户。',
    exportTDSContinueBtn: '知道了',
    exportTDSTitle: '导出 TDS',
    step1: '请选择导出的 TDS 文件中包含的维度列和度量列的范围',
    step2: '请选择导出的 TDS 文件中数据源连接方式',
    exportTDSOptions1: '只包含聚合索引中的维度列和度量列（默认）',
    exportTDSOptions2: '包含聚合索引和明细索引中的维度列和度量列',
    exportTDSOptions3: '包含模型中所有的维度列和度量列，即使这些列没有加入任何聚合索引或明细索引',
    connectODBC: '其他 ODBC 数据源（默认）',
    connectTableau: 'Tableau Kyligence Connector',
    exportTDSOfflineTips: '模型为 OFFLINE 时，导出的 TDS 无法用于查询。请确保使用 TDS 文件时该模型已上线。',
    delModelTip: '你确认要删除模型 {modelName}？',
    delModelTitle: '删除模型',
    pergeModelTip: '你确定要清空模型 {modelName} 的所有 Segment 吗？',
    pergeModelTitle: '清空模型',
    disableModelSuccessTip: '模型成功下线。',
    enabledModelSuccessTip: '模型成功上线。',
    purgeModelSuccessTip: '清理模型成功。',
    deleteModelSuccessTip: '删除模型成功。',
    noSegmentOnlineTip: '该模型尚未添加 Segment，不可服务于查询。请先添加 Segment 后再上线。',
    cannotOnlineTips: '该模型当前不可上线：',
    disableModelTip: '模型下线后将无法服务于查询，已构建的索引可在模型上线后继续使用。确定要下线模型 {modelName} 吗？',
    disableModelTitle: '下线模型',
    noIndexTips: '您需要先添加索引，才可以进行构建',
    changeModelSuccess: '模型 {modelName} 的所有者已成功变更为 {owner}。',
    jobSuccess: '任务已提交。可到任务页',
    disableActionTips: '实时模型暂无法使用此功能',
    disableActionTips2: '融合模型和实时模型暂无法使用此功能',
    disableActionTips3: '融合模型暂无法使用此功能',
    disableActionTips4: '融合模型和实时模型无法修改时间分区设置。',
    disableSecStorageActionTips: '融合模型或实时模型暂无法使用分层存储'
  }
}
