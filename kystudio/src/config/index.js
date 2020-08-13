// 全局配置
import { getFullMapping } from '../util'

let apiUrl
let baseUrl
let regexApiUrl

let pageCount = 10
let pageSizes = [10, 20, 30, 50, 100]

let speedInfoTimer = 6000

let sqlRowsLimit = 100
let sqlStrLenLimit = 2000

let tooltipDelayTime = 400
if (process.env.NODE_ENV === 'development') {
  apiUrl = '/kylin/api/'
  baseUrl = '/kylin/'
  regexApiUrl = '\\/kylin\\/api\\/'
} else {
  apiUrl = '/kylin/api/'
  baseUrl = '/kylin/'
  regexApiUrl = '\\/kylin\\/api\\/'
}
export {
  // http请求
  apiUrl,
  baseUrl,
  regexApiUrl,
  pageCount,
  pageSizes,
  tooltipDelayTime,
  speedInfoTimer,
  sqlRowsLimit,
  sqlStrLenLimit
}
export const menusData = [
  // {name: 'dashboard', path: '/dashboard', icon: 'el-icon-ksd-dashboard'},
  // {name: 'studio', path: '/studio/model', icon: 'el-icon-ksd-studio'},
  // {name: 'auto', path: '/auto', icon: 'el-icon-ksd-auto_modeling'},
  // {name: 'insight', path: '/insight', icon: 'el-icon-ksd-insight'},
  {name: 'dashboard', path: '/dashboard', icon: 'el-icon-ksd-overview'},
  {
    name: 'query',
    path: '/query',
    icon: 'el-icon-ksd-insight',
    children: [
      {name: 'insight', path: '/query/insight'},
      {name: 'queryhistory', path: '/query/queryhistory'}
    ]
  },
  {
    name: 'studio',
    path: '/studio',
    icon: 'el-icon-ksd-studio',
    children: [
      { name: 'source', path: '/studio/source' },
      {name: 'acceleration', path: '/studio/acceleration'},
      { name: 'modelList', path: '/studio/model' }
    ]
  },
  {
    name: 'monitor',
    path: '/monitor',
    icon: 'el-icon-ksd-monitor',
    children: [
      {name: 'job', path: '/monitor/job'}
      // {name: 'cluster', path: '/monitor/cluster'},
      // {name: 'admin', path: '/monitor/admin'}
    ]
  },
  {
    name: 'setting',
    path: '/setting',
    icon: 'el-icon-ksd-setting'
  },
  {
    name: 'project',
    path: '/admin/project',
    icon: 'el-icon-ksd-project_list'
  },
  {
    name: 'user',
    path: '/admin/user',
    icon: 'el-icon-ksd-table_admin'
  },
  {
    name: 'group',
    path: '/admin/group',
    icon: 'el-icon-ksd-table_group'
  },
  {
    name: 'systemcapacity',
    path: '/admin/systemcapacity',
    icon: 'el-icon-ksd-Combined_Shape'
  }
]

export const pageRefTags = {
  indexPager: 'indexPager',
  IndexDetailPager: 'IndexDetailPager',
  tableIndexDetailPager: 'tableIndexDetailPager',
  segmentPager: 'segmentPager',
  capacityPager: 'capacityPager',
  projectDetail: 'projectDetail',
  sqlListsPager: 'sqlListsPager',
  jobPager: 'jobPager',
  queryHistoryPager: 'queryHistoryPager',
  queryResultPager: 'queryResultPager',
  modleConfigPager: 'modleConfigPager',
  batchMeasurePager: 'batchMeasurePager',
  dimensionPager: 'dimensionPager',
  modelListPager: 'modelListPager',
  userPager: 'userPager',
  userGroupPager: 'userGroupPager',
  modelDimensionPager: 'modelDimensionPager',
  modelMeasurePager: 'modelMeasurePager',
  authorityUserPager: 'authorityUserPager',
  authorityTablePager: 'authorityTablePager',
  projectConfigPager: 'projectConfigPager',
  projectPager: 'projectPager',
  confirmSegmentPager: 'confirmSegmentPager',
  addIndexPager: 'addIndexPager',
  indexGroupPager: 'indexGroupPager',
  indexGroupContentShowPager: 'indexGroupContentShowPager',
  tableIndexPager: 'tableIndexPager',
  tableColumnsPager: 'tableColumnsPager',
  statisticsPager: 'statisticsPager',
  recommendationsPager: 'recommendationsPager'
}
export const needLengthMeasureType = ['fixed_length', 'fixed_length_hex', 'int', 'integer']
export const permissions = {
  READ: {name: 'CUBE QUERY', value: 'READ', mask: 1},
  MANAGEMENT: {name: 'CUBE EDIT', value: 'MANAGEMENT', mask: 32},
  OPERATION: {name: 'CUBE OPERATION', value: 'OPERATION', mask: 64},
  ADMINISTRATION: {name: 'CUBE ADMIN', value: 'ADMINISTRATION', mask: 16}
}
export const permissionsMaps = {
  1: 'READ',
  32: 'MANAGEMENT',
  64: 'OPERATION',
  16: 'ADMINISTRATION'
}
export const engineTypeKylin = [
  {name: 'MapReduce', value: 2},
  {name: 'Spark (Beta)', value: 4}
]
export const engineTypeKap = [
  {name: 'MapReduce', value: 100},
  {name: 'Spark (Beta)', value: 98}
]
export const SystemPwdRegex = /^(?=.*\d)(?=.*[a-zA-Z])(?=.*[~!@#$%^&*(){}|:"<>?[\];',./`]).{8,}$/
export const NamedRegex = /^\w+$/
export const NamedRegex1 = /^[\u4e00-\u9fa5_\-（()%?）a-zA-Z0-9\s]+$/
export const positiveNumberRegex = /^[1-9][0-9]*$/ // 的正整数
export const DatePartitionRule = [/^date$/, /^timestamp$/, /^string$/, /^bigint$/, /^int$/, /^integer$/, /^varchar/]
export const TimePartitionRule = [/^long$/, /^bigint$/, /^int$/, /^short$/, /^integer$/, /^tinyint$/, /^string$/, /^varchar/, /^char/]
export const IntegerType = ['bigint', 'int', 'integer', 'tinyint', 'smallint', 'int4', 'long8']
// query result 模块使用解析query 结果排序问题
export const IntegerTypeForQueryResult = ['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL']
export const personalEmail = {
  'qq.com': 'http://mail.qq.com',
  'gmail.com': 'http://mail.google.com',
  'sina.com': 'http://mail.sina.com.cn',
  '163.com': 'http://mail.163.com',
  '126.com': 'http://mail.126.com',
  'yeah.net': 'http://www.yeah.net/',
  'sohu.com': 'http://mail.sohu.com/',
  'tom.com': 'http://mail.tom.com/',
  'sogou.com': 'http://mail.sogou.com/',
  '139.com': 'http://mail.10086.cn/',
  'hotmail.com': 'http://www.hotmail.com',
  'live.com': 'http://login.live.com/',
  'live.cn': 'http://login.live.cn/',
  'live.com.cn': 'http://login.live.com.cn',
  '189.com': 'http://webmail16.189.cn/webmail/',
  'yahoo.com.cn': 'http://mail.cn.yahoo.com/',
  'yahoo.cn': 'http://mail.cn.yahoo.com/',
  'eyou.com': 'http://www.eyou.com/',
  '21cn.com': 'http://mail.21cn.com/',
  '188.com': 'http://www.188.com/',
  'foxmail.com': 'http://www.foxmail.com'
}

export const measuresDataType = [
  'tinyint', 'smallint', 'integer', 'bigint', 'float', 'double', 'decimal', 'timestamp', 'date', 'char', 'varchar', 'boolean'
]

export const measureSumAndTopNDataType = ['tinyint', 'smallint', 'integer', 'bigint', 'float', 'double', 'decimal']

export const measurePercenDataType = ['tinyint', 'smallint', 'integer', 'bigint']

export const timeDataType = [
  'timestamp', 'date', 'time', 'datetime'
]

export const dateFormats = [
  {label: 'yyyy-MM-dd', value: 'yyyy-MM-dd'},
  {label: 'yyyyMMdd', value: 'yyyyMMdd'},
  {label: 'yyyy-MM-dd HH:mm:ss', value: 'yyyy-MM-dd HH:mm:ss'},
  {label: 'yyyy-MM-dd HH:mm:ss.SSS', value: 'yyyy-MM-dd HH:mm:ss.SSS'},
  {label: 'yyyy/MM/dd', value: 'yyyy/MM/dd'},
  {label: 'yyyy-MM', value: 'yyyy-MM'},
  {label: 'yyyyMM', value: 'yyyyMM'}
]

// 根据服务端提供在 issue 的列表，做下数组处理，且进行了去重，最后放到这边的 keywordArr 这个变量中
const keywordArr = [
  'abs',
  'acos',
  'add_months',
  'all',
  'allocate',
  'allow',
  'alter',
  'and',
  'any',
  'are',
  'array',
  'array_max_cardinality',
  'as',
  'asc',
  'asensitive',
  'asin',
  'asymmetric',
  'at',
  'atan',
  'atan2',
  'atomic',
  'authorization',
  'avg',
  'begin',
  'begin_frame',
  'begin_partition',
  'between',
  'bigint',
  'binary',
  'bit',
  'blob',
  'boolean',
  'both',
  'by',
  'call',
  'called',
  'cardinality',
  'cascaded',
  'case',
  'cast',
  'ceil',
  'ceiling',
  'century',
  'char',
  'char_length',
  'character',
  'character_length',
  'check',
  'classifier',
  'clob',
  'close',
  'coalesce',
  'collate',
  'collect',
  'column',
  'commit',
  'concat',
  'condition',
  'connect',
  'constraint',
  'contains',
  'convert',
  'corr',
  'corresponding',
  'cos',
  'cot',
  'count',
  'covar_pop',
  'covar_samp',
  'create',
  'cross',
  'cube',
  'cume_dist',
  'current',
  'current_catalog',
  'current_date',
  'current_default_transform_group',
  'current_path',
  'current_role',
  'current_row',
  'current_schema',
  'current_time',
  'current_timestamp',
  'current_transform_group_for_type',
  'current_user',
  'cursor',
  'cycle',
  'date',
  'datediff',
  'day',
  'dayofmonth',
  'dayofweek',
  'dayofyear',
  'deallocate',
  'dec',
  'decimal',
  'declare',
  'default',
  'define',
  'degrees',
  'delete',
  'dense_rank',
  'deref',
  'desc',
  'describe',
  'deterministic',
  'disallow',
  'disconnect',
  'distinct',
  'double',
  'drop',
  'dynamic',
  'each',
  'element',
  'else',
  'empty',
  'end',
  'end_frame',
  'end_partition',
  'end-exec',
  'equals',
  'escape',
  'every',
  'except',
  'exec',
  'execute',
  'exists',
  'exp',
  'explain',
  'extend',
  'external',
  'extract',
  'false',
  'fetch',
  'filter',
  'first_value',
  'float',
  'floor',
  'for',
  'foreign',
  'frame_row',
  'free',
  'from',
  'full',
  'function',
  'fusion',
  'get',
  'global',
  'grant',
  'group',
  'grouping',
  'groups',
  'having',
  'hold',
  'hour',
  'identity',
  'if',
  'ifnull',
  'import',
  'in',
  'indicator',
  'initcap',
  'initial',
  'inner',
  'inout',
  'insensitive',
  'insert',
  'instr',
  'int',
  'integer',
  'intersect',
  'intersection',
  'interval',
  'into',
  'is',
  'isnull',
  'join',
  'lag',
  'language',
  'large',
  'last_value',
  'lateral',
  'lead',
  'leading',
  'left',
  'length',
  'like',
  'like_regex',
  'limit',
  'ln',
  'local',
  'localtime',
  'localtimestamp',
  'log',
  'log10',
  'lower',
  'ltrim',
  'match',
  'match_number',
  'match_recognize',
  'matches',
  'max',
  'measures',
  'member',
  'merge',
  'method',
  'min',
  'minus',
  'minute',
  'mod',
  'modifies',
  'module',
  'month',
  'multiset',
  'national',
  'natural',
  'nchar',
  'nclob',
  'new',
  'next',
  'no',
  'none',
  'normalize',
  'not',
  'now',
  'nth_value',
  'ntile',
  'null',
  'nullif',
  'numeric',
  'occurrences_regex',
  'octet_length',
  'of',
  'offset',
  'old',
  'omit',
  'on',
  'one',
  'only',
  'open',
  'or',
  'order',
  'out',
  'outer',
  'over',
  'overlaps',
  'overlay',
  'parameter',
  'partition',
  'pattern',
  'per',
  'percent',
  'percent_rank',
  'percentile_cont',
  'percentile_disc',
  'period',
  'permute',
  'pi',
  'portion',
  'position',
  'position_regex',
  'power',
  'precedes',
  'precision',
  'prepare',
  'prev',
  'primary',
  'procedure',
  'quarter',
  'radians',
  'rand',
  'rand_integer',
  'range',
  'rank',
  'reads',
  'real',
  'recursive',
  'ref',
  'references',
  'referencing',
  'regr_avgx',
  'regr_avgy',
  'regr_count',
  'regr_intercept',
  'regr_r2',
  'regr_slope',
  'regr_sxx',
  'regr_sxy',
  'regr_syy',
  'release',
  'repeat',
  'replace',
  'reset',
  'result',
  'return',
  'returns',
  'reverse',
  'revoke',
  'right',
  'rlike',
  'rollback',
  'rollup',
  'round',
  'row',
  'row_number',
  'rows',
  'running',
  'savepoint',
  'scope',
  'scroll',
  'search',
  'second',
  'seek',
  'select',
  'sensitive',
  'session_user',
  'set',
  'show',
  'sign',
  'similar',
  'sin',
  'skip',
  'smallint',
  'some',
  'specific',
  'specifictype',
  'split_part',
  'sql',
  'sql_bigint',
  'sql_binary',
  'sql_bit',
  'sql_blob',
  'sql_boolean',
  'sql_char',
  'sql_clob',
  'sql_date',
  'sql_decimal',
  'sql_double',
  'sql_float',
  'sql_integer',
  'sql_nclob',
  'sql_numeric',
  'sql_nvarchar',
  'sql_real',
  'sql_smallint',
  'sql_time',
  'sql_timestamp',
  'sql_tinyint',
  'sql_tsi_day',
  'sql_tsi_frac_second',
  'sql_tsi_hour',
  'sql_tsi_microsecond',
  'sql_tsi_minute',
  'sql_tsi_month',
  'sql_tsi_quarter',
  'sql_tsi_second',
  'sql_tsi_week',
  'sql_tsi_year',
  'sql_varbinary',
  'sql_varchar',
  'sqlexception',
  'sqlstate',
  'sqlwarning',
  'sqrt',
  'start',
  'static',
  'stddev_pop',
  'stddev_samp',
  'stream',
  'submultiset',
  'subset',
  'substr',
  'substring',
  'substring_regex',
  'succeeds',
  'sum',
  'symmetric',
  'system',
  'system_time',
  'system_user',
  'table',
  'tablesample',
  'tan',
  'then',
  'time',
  'timestamp',
  'timestampadd',
  'timestampdiff',
  'timezone_hour',
  'timezone_minute',
  'tinyint',
  'to',
  'to_date',
  'to_timestamp',
  'trailing',
  'translate',
  'translate_regex',
  'translation',
  'treat',
  'trigger',
  'trim',
  'trim_array',
  'true',
  'truncate',
  'ucase',
  'uescape',
  'union',
  'unique',
  'unknown',
  'unnest',
  'update',
  'upper',
  'upsert',
  'user',
  'using',
  'value',
  'value_of',
  'values',
  'var_pop',
  'var_samp',
  'varbinary',
  'varchar',
  'varying',
  'versioning',
  'view',
  'week',
  'when',
  'whenever',
  'where',
  'width_bucket',
  'window',
  'with',
  'within',
  'without',
  'year'
]
// 将 keywordArr 转化为 编辑器所需的格式
export const insightKeyword = keywordArr.map((item) => {
  return {meta: 'keyword', caption: item, value: item, scope: 1}
})

export const assignTypes = [{value: 'user', label: 'user'}, {value: 'group', label: 'group'}]
export const sourceTypes = getFullMapping({
  HIVE: 9,
  RDBMS: 16,
  KAFKA: 1,
  RDBMS2: 8,
  CSV: 13
})

export const sourceNameMapping = {
  HIVE: 'Hive',
  RDBMS: 'RDBMS',
  KAFKA: 'Kafka',
  RDBMS2: 'RDBMS',
  CSV: 'CSV'
}

export const pageSizeMapping = {
  TABLE_TREE: 10
}

export const speedProjectTypes = [
  'AUTO_MAINTAIN'
]

// 探测影响模型的c操作类别
export const getAffectedModelsType = {
  TOGGLE_PARTITION: 'TOGGLE_PARTITION',
  DROP_TABLE: 'DROP_TABLE',
  RELOAD_ROOT_FACT: 'RELOAD_ROOT_FACT'
}

export { projectCfgs } from './projectCfgs'
