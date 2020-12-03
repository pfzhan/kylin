export default {
  'en': {
    importModelsMetadata: 'Add Model From Imported',
    importModalTips: 'To ensure file integrity, don\'t unzip the file or modify the contents of the file.',
    tableMapLabel: 'Set mapping relationships for the database name and table name',
    tableMapLabelTip: 'You may set the mapping relationships if the database names and the table names are inconsistent in both environments.',
    tableMapPlaceholder: 'Import rules：Item name in export environment = Item name in import environment, Please use comma (,) to separate multiple mapping relationships. Example: DB1=DB2,DB3.Table1=DB3.Table2',
    selectFile: 'Select the file',
    placeholder: 'Please select a .zip file',
    pleaseSelectFile: 'Please select file.',
    parseFile: 'Parse and go to the next step',
    noZipFileErrorTip: 'Only support .zip file. Please select again.',
    unConflictModels: 'Importable models',
    unConflictModelsIsEmpty: 'There is not any importable model.',
    conflictModels: 'Non-importable models ({count})',
    conflictModelsIsEmpty: 'There is not any non-importable model.',
    chooseModels: 'Parse out {count} models',
    pleaseSelectModels: 'Please select at least one model.',
    uploadFailed: 'The file parsing failed. Please check if the model metadata package is complete.',
    submitSuccess: 'Successfully imported model.',
    submitFailed: 'Failed to imported model.',
    filesSizeError: 'File size cannot exceed 5M!',
    DUPLICATE_MODEL_NAME: 'This model already exists.',
    METADATA_CONFLICT: 'The imported model metadata is inconsistent with the data in the project\'s data source.',
    DUPLICATE_MODEL_NAME_AND_METADATA_CONFLICT: 'This model already exists, and metadata is inconsistent with the data in the project\'s data source.',
    TABLE_NOT_EXISTED: 'Tables that do not exist in the data source',
    COLUMN_NOT_EXISTED: 'Columns that do not exist in the data source',
    INVALID_COLUMN_DATATYPE: 'Inconsistent data types',
    parseModalsTips: '{modelNum} models have been parsed successfully',
    parseModalsTips1: ' {noImportNum} models can’t be imported.',
    viewDetails: 'View import rules',
    importTips1: 'The tables and columns referenced by the model have been synced to the system and the data types of the columns are consistent with those in the system.',
    importTips2: 'The table join condition, partition setting, and data filter condition of the model must be consistent with the model have the same name in the system before it can be replaced.',
    importTips3: 'Models with the same name could be chosen to add new or replace the existing ones in the current system',
    modelName: 'Model Name',
    actions: 'Actions',
    addNew: 'Add New',
    replace: 'Replace',
    notImport: 'Not Import',
    nofound: 'Not Found ({num})',
    add: 'Added ({num})',
    reduce: 'Deleted ({num})',
    modified: 'Modified ({num})',
    noDifferences: 'No conflict has been detected for this model.',
    tables: 'Tables ({num})',
    columns: 'Columns ({num})',
    columnsDataType: 'Columns DataType ({num})',
    partitionColumns: 'Partition ({num})',
    measures: 'Measures ({num})',
    dimensions: 'Dimensions ({num})',
    indexes: 'Index ({num})',
    computedColumns: 'Computed Columns ({num})',
    modelJoin: 'Join Condition ({num})',
    modelFilter: 'Data Filter Condition ({num})',
    noFoundDataTip: 'All required tables or columns are found in the current system.',
    noAddDataTip: 'This model has no added items compared to the current system.',
    noDeleteDataTip: 'This model has no deleted items compared to the current system.',
    noUpdateDataTip: 'This model has no modified items compared to the current system.',
    timePartition: 'Time Partition：',
    timePartitionType: 'Time Partition Type：',
    aggIndex: 'Agg Index',
    tableIndex: 'Table Index',
    measureName: 'Measure Name：',
    measureFunction: 'Function：',
    measureFunctionParams: 'Function Parameter：',
    confirmImportTips: 'Please confirm the following operations before import.',
    newConfirmTips: 'Add New ({num})',
    replaceConfirmTips: 'Replace ({num})',
    noImportConfirmTips: 'Not Import ({num})',
    confirmNoImportBtn: 'Not Import',
    confirmImportBtn: 'Import',
    sameCCNameTips: 'There is already a computed column with the same name in the system model, and the expressions are inconsistent. Please modify and import again.',
    sameCCExpressionTips: 'There is already a computed column with the same expressions in the system model, and the names are inconsistent. Please modify and import again.',
    validateErrorTips: 'The file parsing failed. Please check if the model metadata package is complete.'
  },
  'zh-cn': {
    importModelsMetadata: '导入模型',
    importModalTips: '为了确保文件完整性，请勿解压文件或修改文件内容。',
    tableMapLabel: '设置数据库名和表名的映射关系',
    tableMapLabelTip: '当模型导出环境和导入环境的数据库名、表名不一致且存在对应关系时，建议您提前设置对应关系，以免影响模型导入。',
    tableMapPlaceholder: '输入规则：导出环境对象名称=导入环境对象名称，多组映射关系请使用英文逗号(,)分隔。 示例：DB1=DB2,DB3.Table1=DB3.Table2',
    selectFile: '选择文件',
    placeholder: '请选择 .zip 文件',
    pleaseSelectFile: '请选择上传文件。',
    parseFile: '解析并进入下一步',
    noZipFileErrorTip: '仅支持上传 .zip 文件，请重新选择。',
    unConflictModels: '可导入的模型',
    unConflictModelsIsEmpty: '无可导入模型',
    conflictModels: '不可导入的模型 ({count})',
    conflictModelsIsEmpty: '无不可导入模型',
    chooseModels: '共解析出{count}个模型',
    pleaseSelectModels: '请至少选择一个模型。',
    uploadFailed: '解析失败，请检查模型元数据包是否完整',
    submitSuccess: '导入模型成功。',
    submitFailed: '导入模型失败。',
    filesSizeError: '文件大小不能超过 5M！',
    DUPLICATE_MODEL_NAME: '该模型已存在。',
    METADATA_CONFLICT: '导入的模型元数据与该项目的数据源中的数据不一致。',
    DUPLICATE_MODEL_NAME_AND_METADATA_CONFLICT: '该模型已存在，且导入的元数据与该项目的数据源中的数据不一致。',
    TABLE_NOT_EXISTED: '数据源中不存在的表',
    COLUMN_NOT_EXISTED: '数据源中不存在的列',
    INVALID_COLUMN_DATATYPE: '数据类型不一致的列',
    parseModalsTips: '成功解析出 {modelNum} 个模型',
    parseModalsTips1: ' 其中 {noImportNum} 个不可导入。',
    viewDetails: '查看导入规则',
    importTips1: '被模型引用的表、列已同步至系统中且列的数据类型与系统中一致。',
    importTips2: '模型的连接条件、分区列设置、模型数据筛选条件需要与系统中同名模型一致才可覆盖。',
    importTips3: '模型通过名称进行匹配，同名模型可选覆盖或新增',
    modelName: '模型名称',
    actions: '操作',
    addNew: '新建',
    replace: '覆盖',
    notImport: '不导入',
    nofound: '未找到 ({num})',
    add: '增加 ({num})',
    reduce: '删除 ({num})',
    modified: '更改 ({num})',
    noDifferences: '该模型与现有环境无任何冲突',
    tables: '表 ({num})',
    columns: '列 ({num})',
    columnsDataType: '列的数据类型 ({num})',
    partitionColumns: '分区列 ({num})',
    measures: '度量 ({num})',
    dimensions: '维度 ({num})',
    indexes: '索引 ({num})',
    computedColumns: '可计算列 ({num})',
    modelJoin: '连接条件 ({num})',
    modelFilter: '模型过滤条件 ({num})',
    noFoundDataTip: '该模型所引用的表和列均已同步至系统',
    noAddDataTip: '该模型无添加项',
    noDeleteDataTip: '该模型无删除项',
    noUpdateDataTip: '该模型无更改项',
    timePartition: '时间分区：',
    timePartitionType: '时间分区格式：',
    aggIndex: '聚合索引',
    tableIndex: '明细索引',
    measureName: '度量名称：',
    measureFunction: '函数：',
    measureFunctionParams: '函数参数：',
    confirmImportTips: '在导入前，请再次确认对导入模型的操作。',
    newConfirmTips: '新建 ({num})',
    replaceConfirmTips: '覆盖 ({num})',
    noImportConfirmTips: '不导入 ({num})',
    confirmNoImportBtn: '确定不导入',
    confirmImportBtn: '确定导入',
    sameCCNameTips: '系统中模型已存在相同名称的可计算列，且两者表达式不一致，请修改后再导入此模型。',
    sameCCExpressionTips: '系统中模型已存在相同表达式的可计算列，且两者名称不一致，请修改后再导入此模型。',
    validateErrorTips: '解析失败，请检查模型数据包是否完整。'
  }
}
