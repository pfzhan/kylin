export default {
  'en': {
    projectName: 'Project Name',
    projectType: 'Project Type',
    description: 'Description',
    AUTO_MAINTAIN: 'Smart Mode',
    MANUAL_MAINTAIN: 'Expert Mode',
    fileBased: 'File-based source',
    fileBasedDesc: 'Allow to add data file (csv/txt/json/orc) as source and store them in your filesystem.',
    sourceSampling: 'Source Sampling',
    sourceSamplingDesc: 'Default to do table sampling once you add a new table from filesystem-based data source.',
    pushdownEngin: 'Pushdown Engine',
    pushdownRange: 'Pushdown Range',
    pushdownRangeDesc: 'To keep query result consistent, system by default regards data range as the source table\'s pushdown range.'
  },
  'zh-cn': {
    projectName: '项目名称',
    projectType: '项目类型',
    description: '描述',
    AUTO_MAINTAIN: '智能模式',
    MANUAL_MAINTAIN: '专业模式',
    fileBased: '数据源来自文件',
    fileBasedDesc: '允许从文件中（csv/txt/json/orc）提取数据作为数据源，并且存储在文件系统中。',
    sourceSampling: '数据源采样',
    sourceSamplingDesc: '当文件型数据源添加数据源表时，默认进行表采样。',
    pushdownEngin: '下压查询引擎',
    pushdownRange: '下压查询范围',
    pushdownRangeDesc: '为了保持查询的可持续性，系统默认保持数据范围和源表的下压查询范围一致。'
  }
}
