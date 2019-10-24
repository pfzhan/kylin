export default {
  'en': {
    dialogHiveTreeLoading: 'loading',
    loadTipHeader: 'How to load source table metadata',
    loadTip1: 'Enter table name as \'database.table\': if you don\'t need to take a look at tables, just enter table name as \'database.table\'; use comma to separate multiple tables\' name; use ENTER to close entering. The maximum is 1000 tables.',
    loadTip2: 'Select tables from tree: click tables in the left tree and the maximum is 1,000 tables.',
    loadTip3: 'Input manually: input the database or table name; use ENTER to complete and comma to separate multiple values.',
    loadHiveTipHeader: 'How to load source table metadata',
    loadHiveTip1: 'Select table: unfold the left source tree and choose a table by clicking it;',
    loadHiveTip2: 'Select database: choose a database will select all tables below it, each batch can load up to 1000 tables;',
    sampling: 'Table Sampling',
    selectedHiveValidateFailText: 'Please enter table name as \'database.table\'.',
    selectAll: 'Select All',
    cleanAll: 'Clean All',
    database: 'Database',
    tableName: 'Table',
    synced: 'Synced',
    filterTableName: 'Search Database/Table',
    samplingTitle: 'Table Sampling ',
    sampleDesc: 'The system will sample all loaded tables.',
    sampleDesc1: 'Sampling range should not exceed ',
    sampleDesc2: ' rows.',
    invalidType: 'Please input an integer',
    minNumber: 'Input should be no less than 10,000 rows',
    maxNumber: 'Input should be no larger than 20,000,000 rows',
    selectedDBValidateFailText: 'Please enter database name',
    selectedTableValidateFailText: 'Please enter table name as \'database.table\'.',
    dbPlaceholder: 'Enter database name or select from the left',
    dbTablePlaceholder: 'Enter \'database.table\' or select from the left',
    refreshNow: 'Refresh now',
    refreshIng: 'Refreshing',
    refreshText: 'Can\'t find what you\'re looking for?',
    refreshTips: 'The system caches the source table metadata periodically. If you can\'t find what you\'re looking for, you can refresh immediately or wait for the system to finish refreshing.'
  },
  'zh-cn': {
    dialogHiveTreeLoading: '加载中',
    loadTipHeader: '加载源表元数据的方式',
    loadTip1: '单表选择：展开左侧的数据树，点击选择单个数据表；',
    loadTip2: '批量选择：点击数据库将批量选中下面所有的表，每一批加载表上限为 1000 张表；',
    loadTip3: '手动输入：输入数据库名或表名，按回车键进行确认。当输入多个值时，请用逗号分割。',
    loadHiveTipHeader: '加载源表元数据的方式',
    loadHiveTip1: '单表选择：展开左侧的数据树，点击选择单个数据表；',
    loadHiveTip2: '批量选择：点击数据库将批量选中下面所有的表，每一批加载表上限为 1000 张表；',
    sampling: '表抽样',
    selectedHiveValidateFailText: '请输入完整表名\'database.table\'。',
    selectAll: '选中所有',
    cleanAll: '清除所有',
    database: '数据库',
    tableName: '数据源表',
    synced: '已同步',
    filterTableName: '搜索数据库名称或表名',
    samplingTitle: '表级数据抽样',
    sampleDesc: '系统将对所有加载的表进行全表抽样。',
    sampleDesc1: '数据抽样范围不超过',
    sampleDesc2: '行。',
    invalidType: '请输入一个整数',
    minNumber: '输入值应不小于 10,000 行',
    maxNumber: '输入值应 不大于 20,000,000 行',
    selectedDBValidateFailText: '请输入完整数据库名',
    selectedTableValidateFailText: '请输入完整表名\'database.table\'。',
    dbPlaceholder: '输入数据库名称或从左侧选择',
    dbTablePlaceholder: '输入 \'database.table\' 或从左侧选择',
    refreshNow: '立即刷新',
    refreshIng: '正在刷新',
    refreshText: '未找到想要搜索的内容？',
    refreshTips: '系统会定时缓存源表元数据。如果未找到搜索结果，可以立即刷新，或等待系统刷新完毕。'
  }
}
