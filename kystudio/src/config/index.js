// 全局配置
import { getFullMapping } from '../util'

let apiUrl
let baseUrl
let regexApiUrl

let pageCount = 10
let pageSizes = [5, 10, 20, 30, 40]

let speedInfoTimer = 6000

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
  speedInfoTimer
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
      {name: 'insight', path: '/query/new_query'},
      {name: 'query_history', path: '/query/query_history'}
    ]
  },
  {
    name: 'studio',
    path: '/studio',
    icon: 'el-icon-ksd-studio',
    children: [
      { name: 'source', path: '/studio/source' },
      {name: 'favorite_query', path: '/studio/favorite_query'},
      { name: 'model', path: '/studio/model' }
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
  // {
  //   name: 'setting',
  //   path: '/setting',
  //   icon: 'el-icon-ksd-setting'
  // },
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
  }
]
export const modelHealthStatus = {
  'RUNNING': {
    icon: '',
    color: '',
    message: 'Job Running'
  },
  'GOOD': {
    icon: 'el-icon-success',
    color: '#4cb050',
    message: 'Good Health'
  },
  'WARN': {
    icon: 'el-icon-warning',
    color: '#f7ba2a',
    message: ''
  },
  'BAD': {
    icon: 'el-icon-error',
    color: '#ff4159',
    message: ''
  },
  'TERRIBLE': {
    icon: 'el-icon-error',
    color: '#ff4159',
    message: ''
  },
  'NONE': {
    icon: 'el-icon-question',
    color: '#cfd8dc',
    message: 'This model has no check result'
  },
  'ERROR': {
    icon: 'circle-o-notch',
    color: '#ff4159',
    message: 'Check job failed'
  }
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

export const computedDataType = [
  'tinyint', 'smallint', 'integer', 'bigint', 'float', 'double', 'decimal', 'timestamp', 'date', 'varchar', 'boolean'
]

export const measuresDataType = [
  'tinyint', 'smallint', 'integer', 'int', 'bigint', 'float', 'double', 'decimal', 'timestamp', 'date', 'varchar', 'boolean'
]

export const timeDataType = [
  'timestamp', 'date', 'time', 'datetime'
]

export const insightKeyword = [
  {meta: 'keyword', caption: 'abort', value: 'abort', scope: 1},
  {meta: 'keyword', caption: 'abs', value: 'abs', scope: 1},
  {meta: 'keyword', caption: 'add', value: 'add', scope: 1},
  {meta: 'keyword', caption: 'admin', value: 'admin', scope: 1},
  {meta: 'keyword', caption: 'acos', value: 'acos', scope: 1},
  {meta: 'keyword', caption: 'add_months', value: 'add_months', scope: 1},
  {meta: 'keyword', caption: 'after', value: 'after', scope: 1},
  {meta: 'keyword', caption: 'all', value: 'all', scope: 1},
  {meta: 'keyword', caption: 'alter', value: 'alter', scope: 1},
  {meta: 'keyword', caption: 'analyze', value: 'analyze', scope: 1},
  {meta: 'keyword', caption: 'archive', value: 'archive', scope: 1},
  {meta: 'keyword', caption: 'array', value: 'array', scope: 1},
  {meta: 'keyword', caption: 'array_contains', value: 'array_contains', scope: 1},
  {meta: 'keyword', caption: 'ascii', value: 'ascii', scope: 1},
  {meta: 'keyword', caption: 'asin', value: 'asin', scope: 1},
  {meta: 'keyword', caption: 'assert_true', value: 'assert_true', scope: 1},
  {meta: 'keyword', caption: 'authorization', value: 'authorization', scope: 1},
  {meta: 'keyword', caption: 'autocommit', value: 'autocommit', scope: 1},
  {meta: 'keyword', caption: 'atan', value: 'atan', scope: 1},
  {meta: 'keyword', caption: 'base64', value: 'base64', scope: 1},
  {meta: 'keyword', caption: 'before', value: 'before', scope: 1},
  {meta: 'keyword', caption: 'between', value: 'between', scope: 1},
  {meta: 'keyword', caption: 'bin', value: 'bin', scope: 1},
  {meta: 'keyword', caption: 'boolean', value: 'boolean', scope: 1},
  {meta: 'keyword', caption: 'both', value: 'both', scope: 1},
  {meta: 'keyword', caption: 'bucket', value: 'bucket', scope: 1},
  {meta: 'keyword', caption: 'buckets', value: 'buckets', scope: 1},
  {meta: 'keyword', caption: 'by', value: 'by', scope: 1},
  {meta: 'keyword', caption: 'cache', value: 'cache', scope: 1},
  {meta: 'keyword', caption: 'cascade', value: 'cascade', scope: 1},
  {meta: 'keyword', caption: 'cast', value: 'cast', scope: 1},
  {meta: 'keyword', caption: 'cbrt', value: 'cbrt', scope: 1},
  {meta: 'keyword', caption: 'ceil', value: 'ceil', scope: 1},
  {meta: 'keyword', caption: 'ceiling', value: 'ceiling', scope: 1},
  {meta: 'keyword', caption: 'change', value: 'change', scope: 1},
  {meta: 'keyword', caption: 'cluster', value: 'cluster', scope: 1},
  {meta: 'keyword', caption: 'clustered', value: 'clustered', scope: 1},
  {meta: 'keyword', caption: 'clusterstatus', value: 'clusterstatus', scope: 1},
  {meta: 'keyword', caption: 'coalesce', value: 'coalesce', scope: 1},
  {meta: 'keyword', caption: 'collect_list', value: 'collect_list', scope: 1},
  {meta: 'keyword', caption: 'collect_set', value: 'collect_set', scope: 1},
  {meta: 'keyword', caption: 'collection', value: 'collection', scope: 1},
  {meta: 'keyword', caption: 'column', value: 'column', scope: 1},
  {meta: 'keyword', caption: 'columns', value: 'columns', scope: 1},
  {meta: 'keyword', caption: 'comment', value: 'comment', scope: 1},
  {meta: 'keyword', caption: 'commit', value: 'commit', scope: 1},
  {meta: 'keyword', caption: 'compact', value: 'compact', scope: 1},
  {meta: 'keyword', caption: 'compactions', value: 'compactions', scope: 1},
  {meta: 'keyword', caption: 'compute', value: 'compute', scope: 1},
  {meta: 'keyword', caption: 'compute_stats', value: 'compute_stats', scope: 1},
  {meta: 'keyword', caption: 'concat', value: 'concat', scope: 1},
  {meta: 'keyword', caption: 'concat_ws', value: 'concat_ws', scope: 1},
  {meta: 'keyword', caption: 'concatenate', value: 'concatenate', scope: 1},
  {meta: 'keyword', caption: 'conf', value: 'conf', scope: 1},
  {meta: 'keyword', caption: 'constraint', value: 'constraint', scope: 1},
  {meta: 'keyword', caption: 'context_ngrams', value: 'context_ngrams', scope: 1},
  {meta: 'keyword', caption: 'continue', value: 'continue', scope: 1},
  {meta: 'keyword', caption: 'conv', value: 'conv', scope: 1},
  {meta: 'keyword', caption: 'corr', value: 'corr', scope: 1},
  {meta: 'keyword', caption: 'cos', value: 'cos', scope: 1},
  {meta: 'keyword', caption: 'covar_pop', value: 'covar_pop', scope: 1},
  {meta: 'keyword', caption: 'covar_samp', value: 'covar_samp', scope: 1},
  {meta: 'keyword', caption: 'create_union', value: 'create_union', scope: 1},
  {meta: 'keyword', caption: 'cube', value: 'cube', scope: 1},
  {meta: 'keyword', caption: 'cume_dist', value: 'cume_dist', scope: 1},
  {meta: 'keyword', caption: 'current', value: 'current', scope: 1},
  {meta: 'keyword', caption: 'current_database', value: 'current_database', scope: 1},
  {meta: 'keyword', caption: 'current_date', value: 'current_date', scope: 1},
  {meta: 'keyword', caption: 'current_timestamp', value: 'current_timestamp', scope: 1},
  {meta: 'keyword', caption: 'current_user', value: 'current_user', scope: 1},
  {meta: 'keyword', caption: 'cursor', value: 'cursor', scope: 1},
  {meta: 'keyword', caption: 'data', value: 'data', scope: 1},
  {meta: 'keyword', caption: 'databases', value: 'databases', scope: 1},
  {meta: 'keyword', caption: 'date_add', value: 'date_add', scope: 1},
  {meta: 'keyword', caption: 'date_format', value: 'date_format', scope: 1},
  {meta: 'keyword', caption: 'date_sub', value: 'date_sub', scope: 1},
  {meta: 'keyword', caption: 'datediff', value: 'datediff', scope: 1},
  {meta: 'keyword', caption: 'datetime', value: 'datetime', scope: 1},
  {meta: 'keyword', caption: 'day', value: 'day', scope: 1},
  {meta: 'keyword', caption: 'days', value: 'days', scope: 1},
  {meta: 'keyword', caption: 'dayofmonth', value: 'dayofmonth', scope: 1},
  {meta: 'keyword', caption: 'dayofweek', value: 'dayofweek', scope: 1},
  {meta: 'keyword', caption: 'dbproperties', value: 'dbproperties', scope: 1},
  {meta: 'keyword', caption: 'deferred', value: 'deferred', scope: 1},
  {meta: 'keyword', caption: 'defined', value: 'defined', scope: 1},
  {meta: 'keyword', caption: 'decode', value: 'decode', scope: 1},
  {meta: 'keyword', caption: 'degrees', value: 'degrees', scope: 1},
  {meta: 'keyword', caption: 'delimited', value: 'delimited', scope: 1},
  {meta: 'keyword', caption: 'dense_rank', value: 'dense_rank', scope: 1},
  {meta: 'keyword', caption: 'dependency', value: 'dependency', scope: 1},
  {meta: 'keyword', caption: 'describe', value: 'describe', scope: 1},
  {meta: 'keyword', caption: 'detail', value: 'detail', scope: 1},
  {meta: 'keyword', caption: 'directories', value: 'directories', scope: 1},
  {meta: 'keyword', caption: 'directory', value: 'directory', scope: 1},
  {meta: 'keyword', caption: 'disable', value: 'disable', scope: 1},
  {meta: 'keyword', caption: 'distinct', value: 'distinct', scope: 1},
  {meta: 'keyword', caption: 'distribute', value: 'distribute', scope: 1},
  {meta: 'keyword', caption: 'dow', value: 'dow', scope: 1},
  {meta: 'keyword', caption: 'drop', value: 'drop', scope: 1},
  {meta: 'keyword', caption: 'e', value: 'e', scope: 1},
  {meta: 'keyword', caption: 'elem_type', value: 'elem_type', scope: 1},
  {meta: 'keyword', caption: 'elt', value: 'elt', scope: 1},
  {meta: 'keyword', caption: 'enable', value: 'enable', scope: 1},
  {meta: 'keyword', caption: 'encode', value: 'encode', scope: 1},
  {meta: 'keyword', caption: 'escaped', value: 'escaped', scope: 1},
  {meta: 'keyword', caption: 'ewah_bitmap', value: 'ewah_bitmap', scope: 1},
  {meta: 'keyword', caption: 'ewah_bitmap_and', value: 'ewah_bitmap_and', scope: 1},
  {meta: 'keyword', caption: 'ewah_bitmap_empty', value: 'ewah_bitmap_empty', scope: 1},
  {meta: 'keyword', caption: 'ewah_bitmap_or', value: 'ewah_bitmap_or', scope: 1},
  {meta: 'keyword', caption: 'exchange', value: 'exchange', scope: 1},
  {meta: 'keyword', caption: 'exclusive', value: 'exclusive', scope: 1},
  {meta: 'keyword', caption: 'exists', value: 'exists', scope: 1},
  {meta: 'keyword', caption: 'exp', value: 'exp', scope: 1},
  {meta: 'keyword', caption: 'explain', value: 'explain', scope: 1},
  {meta: 'keyword', caption: 'explode', value: 'explode', scope: 1},
  {meta: 'keyword', caption: 'export', value: 'export', scope: 1},
  {meta: 'keyword', caption: 'expression', value: 'expression', scope: 1},
  {meta: 'keyword', caption: 'extended', value: 'extended', scope: 1},
  {meta: 'keyword', caption: 'external', value: 'external', scope: 1},
  {meta: 'keyword', caption: 'extract', value: 'extract', scope: 1},
  {meta: 'keyword', caption: 'factorial', value: 'factorial', scope: 1},
  {meta: 'keyword', caption: 'false', value: 'false', scope: 1},
  {meta: 'keyword', caption: 'fetch', value: 'fetch', scope: 1},
  {meta: 'keyword', caption: 'field', value: 'field', scope: 1},
  {meta: 'keyword', caption: 'fields', value: 'fields', scope: 1},
  {meta: 'keyword', caption: 'file', value: 'file', scope: 1},
  {meta: 'keyword', caption: 'fileformat', value: 'fileformat', scope: 1},
  {meta: 'keyword', caption: 'find_in_set', value: 'find_in_set', scope: 1},
  {meta: 'keyword', caption: 'first', value: 'first', scope: 1},
  {meta: 'keyword', caption: 'first_value', value: 'first_value', scope: 1},
  {meta: 'keyword', caption: 'float', value: 'float', scope: 1},
  {meta: 'keyword', caption: 'floor', value: 'floor', scope: 1},
  {meta: 'keyword', caption: 'following', value: 'following', scope: 1},
  {meta: 'keyword', caption: 'for', value: 'for', scope: 1},
  {meta: 'keyword', caption: 'format_number', value: 'format_number', scope: 1},
  {meta: 'keyword', caption: 'formatted', value: 'formatted', scope: 1},
  {meta: 'keyword', caption: 'from_unixtime', value: 'from_unixtime', scope: 1},
  {meta: 'keyword', caption: 'from_utc_timestamp', value: 'from_utc_timestamp', scope: 1},
  {meta: 'keyword', caption: 'full', value: 'full', scope: 1},
  {meta: 'keyword', caption: 'function', value: 'function', scope: 1},
  {meta: 'keyword', caption: 'functions', value: 'functions', scope: 1},
  {meta: 'keyword', caption: 'get_json_object', value: 'get_json_object', scope: 1},
  {meta: 'keyword', caption: 'greatest', value: 'greatest', scope: 1},
  {meta: 'keyword', caption: 'grouping', value: 'grouping', scope: 1},
  {meta: 'keyword', caption: 'hash', value: 'hash', scope: 1},
  {meta: 'keyword', caption: 'having', value: 'having', scope: 1},
  {meta: 'keyword', caption: 'hex', value: 'hex', scope: 1},
  {meta: 'keyword', caption: 'histogram_numeric', value: 'histogram_numeric', scope: 1},
  {meta: 'keyword', caption: 'hold_ddltime', value: 'hold_ddltime', scope: 1},
  {meta: 'keyword', caption: 'hour', value: 'hour', scope: 1},
  {meta: 'keyword', caption: 'hours', value: 'hours', scope: 1},
  {meta: 'keyword', caption: 'idxproperties', value: 'idxproperties', scope: 1},
  {meta: 'keyword', caption: 'if', value: 'if', scope: 1},
  {meta: 'keyword', caption: 'ignore', value: 'ignore', scope: 1},
  {meta: 'keyword', caption: 'import', value: 'import', scope: 1},
  {meta: 'keyword', caption: 'in', value: 'in', scope: 1},
  {meta: 'keyword', caption: 'in_file', value: 'in_file', scope: 1},
  {meta: 'keyword', caption: 'index', value: 'index', scope: 1},
  {meta: 'keyword', caption: 'indexes', value: 'indexes', scope: 1},
  {meta: 'keyword', caption: 'initcap', value: 'initcap', scope: 1},
  {meta: 'keyword', caption: 'inline', value: 'inline', scope: 1},
  {meta: 'keyword', caption: 'inpath', value: 'inpath', scope: 1},
  {meta: 'keyword', caption: 'inputdriver', value: 'inputdriver', scope: 1},
  {meta: 'keyword', caption: 'inputformat', value: 'inputformat', scope: 1},
  {meta: 'keyword', caption: 'instr', value: 'instr', scope: 1},
  {meta: 'keyword', caption: 'intersect', value: 'intersect', scope: 1},
  {meta: 'keyword', caption: 'interval', value: 'interval', scope: 1},
  {meta: 'keyword', caption: 'into', value: 'into', scope: 1},
  {meta: 'keyword', caption: 'is', value: 'is', scope: 1},
  {meta: 'keyword', caption: 'isolation', value: 'isolation', scope: 1},
  {meta: 'keyword', caption: 'isnotnull', value: 'isnotnull', scope: 1},
  {meta: 'keyword', caption: 'items', value: 'items', scope: 1},
  {meta: 'keyword', caption: 'jar', value: 'jar', scope: 1},
  {meta: 'keyword', caption: 'java_method', value: 'java_method', scope: 1},
  {meta: 'keyword', caption: 'join', value: 'join', scope: 1},
  {meta: 'keyword', caption: 'json_tuple', value: 'json_tuple', scope: 1},
  {meta: 'keyword', caption: 'keys', value: 'keys', scope: 1},
  {meta: 'keyword', caption: 'key_type', value: 'key_type', scope: 1},
  {meta: 'keyword', caption: 'lag', value: 'lag', scope: 1},
  {meta: 'keyword', caption: 'last_day', value: 'last_day', scope: 1},
  {meta: 'keyword', caption: 'last_value', value: 'last_value', scope: 1},
  {meta: 'keyword', caption: 'lateral', value: 'lateral', scope: 1},
  {meta: 'keyword', caption: 'lead', value: 'lead', scope: 1},
  {meta: 'keyword', caption: 'least', value: 'least', scope: 1},
  {meta: 'keyword', caption: 'length', value: 'length', scope: 1},
  {meta: 'keyword', caption: 'less', value: 'less', scope: 1},
  {meta: 'keyword', caption: 'level', value: 'level', scope: 1},
  {meta: 'keyword', caption: 'levenshtein', value: 'levenshtein', scope: 1},
  {meta: 'keyword', caption: 'like', value: 'like', scope: 1},
  {meta: 'keyword', caption: 'lines', value: 'lines', scope: 1},
  {meta: 'keyword', caption: 'ln', value: 'ln', scope: 1},
  {meta: 'keyword', caption: 'load', value: 'load', scope: 1},
  {meta: 'keyword', caption: 'local', value: 'local', scope: 1},
  {meta: 'keyword', caption: 'locate', value: 'locate', scope: 1},
  {meta: 'keyword', caption: 'location', value: 'location', scope: 1},
  {meta: 'keyword', caption: 'lock', value: 'lock', scope: 1},
  {meta: 'keyword', caption: 'locks', value: 'locks', scope: 1},
  {meta: 'keyword', caption: 'log', value: 'log', scope: 1},
  {meta: 'keyword', caption: 'log10', value: 'log10', scope: 1},
  {meta: 'keyword', caption: 'log2', value: 'log2', scope: 1},
  {meta: 'keyword', caption: 'logical', value: 'logical', scope: 1},
  {meta: 'keyword', caption: 'long', value: 'long', scope: 1},
  {meta: 'keyword', caption: 'lower', value: 'lower', scope: 1},
  {meta: 'keyword', caption: 'lpad', value: 'lpad', scope: 1},
  {meta: 'keyword', caption: 'ltrim', value: 'ltrim', scope: 1},
  {meta: 'keyword', caption: 'macro', value: 'macro', scope: 1},
  {meta: 'keyword', caption: 'map', value: 'map', scope: 1},
  {meta: 'keyword', caption: 'mapjoin', value: 'mapjoin', scope: 1},
  {meta: 'keyword', caption: 'map_keys', value: 'map_keys', scope: 1},
  {meta: 'keyword', caption: 'map_values', value: 'map_values', scope: 1},
  {meta: 'keyword', caption: 'materialized', value: 'materialized', scope: 1},
  {meta: 'keyword', caption: 'metadata', value: 'metadata', scope: 1},
  {meta: 'keyword', caption: 'minus', value: 'minus', scope: 1},
  {meta: 'keyword', caption: 'minute', value: 'minute', scope: 1},
  {meta: 'keyword', caption: 'minutes', value: 'minutes', scope: 1},
  {meta: 'keyword', caption: 'month', value: 'month', scope: 1},
  {meta: 'keyword', caption: 'months', value: 'months', scope: 1},
  {meta: 'keyword', caption: 'months_between', value: 'months_between', scope: 1},
  {meta: 'keyword', caption: 'more', value: 'more', scope: 1},
  {meta: 'keyword', caption: 'msck', value: 'msck', scope: 1},
  {meta: 'keyword', caption: 'named_struct', value: 'named_struct', scope: 1},
  {meta: 'keyword', caption: 'negative', value: 'negative', scope: 1},
  {meta: 'keyword', caption: 'next_day', value: 'next_day', scope: 1},
  {meta: 'keyword', caption: 'ngrams', value: 'ngrams', scope: 1},
  {meta: 'keyword', caption: 'no_drop', value: 'no_drop', scope: 1},
  {meta: 'keyword', caption: 'none', value: 'none', scope: 1},
  {meta: 'keyword', caption: 'noop', value: 'noop', scope: 1},
  {meta: 'keyword', caption: 'noopstreaming', value: 'noopstreaming', scope: 1},
  {meta: 'keyword', caption: 'noopwithmap', value: 'noopwithmap', scope: 1},
  {meta: 'keyword', caption: 'noopwithmapstreaming', value: 'noopwithmapstreaming', scope: 1},
  {meta: 'keyword', caption: 'norely', value: 'norely', scope: 1},
  {meta: 'keyword', caption: 'noscan', value: 'noscan', scope: 1},
  {meta: 'keyword', caption: 'novalidate', value: 'novalidate', scope: 1},
  {meta: 'keyword', caption: 'ntile', value: 'ntile', scope: 1},
  {meta: 'keyword', caption: 'nulls', value: 'nulls', scope: 1},
  {meta: 'keyword', caption: 'nvl', value: 'nvl', scope: 1},
  {meta: 'keyword', caption: 'of', value: 'of', scope: 1},
  {meta: 'keyword', caption: 'offline', value: 'offline', scope: 1},
  {meta: 'keyword', caption: 'only', value: 'only', scope: 1},
  {meta: 'keyword', caption: 'operator', value: 'operator', scope: 1},
  {meta: 'keyword', caption: 'option', value: 'option', scope: 1},
  {meta: 'keyword', caption: 'out', value: 'out', scope: 1},
  {meta: 'keyword', caption: 'outputdriver', value: 'outputdriver', scope: 1},
  {meta: 'keyword', caption: 'outputformat', value: 'outputformat', scope: 1},
  {meta: 'keyword', caption: 'over', value: 'over', scope: 1},
  {meta: 'keyword', caption: 'overwrite', value: 'overwrite', scope: 1},
  {meta: 'keyword', caption: 'owner', value: 'owner', scope: 1},
  {meta: 'keyword', caption: 'parse_url', value: 'parse_url', scope: 1},
  {meta: 'keyword', caption: 'parse_url_tuple', value: 'parse_url_tuple', scope: 1},
  {meta: 'keyword', caption: 'partialscan', value: 'partialscan', scope: 1},
  {meta: 'keyword', caption: 'partition', value: 'partition', scope: 1},
  {meta: 'keyword', caption: 'partitioned', value: 'partitioned', scope: 1},
  {meta: 'keyword', caption: 'partitions', value: 'partitions', scope: 1},
  {meta: 'keyword', caption: 'percent', value: 'percent', scope: 1},
  {meta: 'keyword', caption: 'percent_rank', value: 'percent_rank', scope: 1},
  {meta: 'keyword', caption: 'percentile', value: 'percentile', scope: 1},
  {meta: 'keyword', caption: 'percentile_approx', value: 'percentile_approx', scope: 1},
  {meta: 'keyword', caption: 'pi', value: 'pi', scope: 1},
  {meta: 'keyword', caption: 'plus', value: 'plus', scope: 1},
  {meta: 'keyword', caption: 'pmod', value: 'pmod', scope: 1},
  {meta: 'keyword', caption: 'posexplode', value: ' posexplode', scope: 1},
  {meta: 'keyword', caption: 'power', value: 'power', scope: 1},
  {meta: 'keyword', caption: 'positive', value: 'positive', scope: 1},
  {meta: 'keyword', caption: 'pow', value: 'pow', scope: 1},
  {meta: 'keyword', caption: 'preceding', value: 'preceding', scope: 1},
  {meta: 'keyword', caption: 'precision', value: 'precision', scope: 1},
  {meta: 'keyword', caption: 'preserve', value: 'preserve', scope: 1},
  {meta: 'keyword', caption: 'pretty', value: 'pretty', scope: 1},
  {meta: 'keyword', caption: 'printf', value: 'printf', scope: 1},
  {meta: 'keyword', caption: 'principals', value: 'principals', scope: 1},
  {meta: 'keyword', caption: 'procedure', value: 'procedure', scope: 1},
  {meta: 'keyword', caption: 'protection', value: 'protection', scope: 1},
  {meta: 'keyword', caption: 'purge', value: 'purge', scope: 1},
  {meta: 'keyword', caption: 'quarter', value: 'quarter', scope: 1},
  {meta: 'keyword', caption: 'radians', value: 'radians', scope: 1},
  {meta: 'keyword', caption: 'rand', value: 'rand', scope: 1},
  {meta: 'keyword', caption: 'range', value: 'range', scope: 1},
  {meta: 'keyword', caption: 'read', value: 'read', scope: 1},
  {meta: 'keyword', caption: 'reads', value: 'reads', scope: 1},
  {meta: 'keyword', caption: 'readonly', value: 'readonly', scope: 1},
  {meta: 'keyword', caption: 'rebuild', value: 'rebuild', scope: 1},
  {meta: 'keyword', caption: 'recordreader', value: 'recordreader', scope: 1},
  {meta: 'keyword', caption: 'recordwriter', value: 'recordwriter', scope: 1},
  {meta: 'keyword', caption: 'reduce', value: 'reduce', scope: 1},
  {meta: 'keyword', caption: 'reference', value: 'reference', scope: 1},
  {meta: 'keyword', caption: 'reflect', value: 'reflect', scope: 1},
  {meta: 'keyword', caption: 'reflect2', value: 'reflect2', scope: 1},
  {meta: 'keyword', caption: 'regexp', value: 'regexp', scope: 1},
  {meta: 'keyword', caption: 'regexp_extract', value: 'regexp_extract', scope: 1},
  {meta: 'keyword', caption: 'regexp_replace', value: 'regexp_replace', scope: 1},
  {meta: 'keyword', caption: 'reload', value: 'reload', scope: 1},
  {meta: 'keyword', caption: 'rely', value: 'rely', scope: 1},
  {meta: 'keyword', caption: 'rename', value: 'rename', scope: 1},
  {meta: 'keyword', caption: 'repeat', value: 'repeat', scope: 1},
  {meta: 'keyword', caption: 'repair', value: 'repair', scope: 1},
  {meta: 'keyword', caption: 'replace', value: 'replace', scope: 1},
  {meta: 'keyword', caption: 'replication', value: 'replication', scope: 1},
  {meta: 'keyword', caption: 'restrict', value: 'restrict', scope: 1},
  {meta: 'keyword', caption: 'reverse', value: 'reverse', scope: 1},
  {meta: 'keyword', caption: 'revoke', value: 'revoke', scope: 1},
  {meta: 'keyword', caption: 'rewrite', value: 'rewrite', scope: 1},
  {meta: 'keyword', caption: 'right', value: 'right', scope: 1},
  {meta: 'keyword', caption: 'rlike', value: 'rlike', scope: 1},
  {meta: 'keyword', caption: 'role', value: 'role', scope: 1},
  {meta: 'keyword', caption: 'roles', value: 'roles', scope: 1},
  {meta: 'keyword', caption: 'rollback', value: 'rollback', scope: 1},
  {meta: 'keyword', caption: 'rollup', value: 'rollup', scope: 1},
  {meta: 'keyword', caption: 'row', value: 'row', scope: 1},
  {meta: 'keyword', caption: 'rows', value: 'rows', scope: 1},
  {meta: 'keyword', caption: 'row_number', value: 'row_number', scope: 1},
  {meta: 'keyword', caption: 'rpad', value: 'rpad', scope: 1},
  {meta: 'keyword', caption: 'rtrim', value: 'rtrim', scope: 1},
  {meta: 'keyword', caption: 'schema', value: 'schema', scope: 1},
  {meta: 'keyword', caption: 'schemas', value: 'schemas', scope: 1},
  {meta: 'keyword', caption: 'second', value: 'second', scope: 1},
  {meta: 'keyword', caption: 'seconds', value: 'seconds', scope: 1},
  {meta: 'keyword', caption: 'select', value: 'select', scope: 1},
  {meta: 'keyword', caption: 'semi', value: 'semi', scope: 1},
  {meta: 'keyword', caption: 'sentences', value: 'sentences', scope: 1},
  {meta: 'keyword', caption: 'serde', value: 'serde', scope: 1},
  {meta: 'keyword', caption: 'serdeproperties', value: 'serdeproperties', scope: 1},
  {meta: 'keyword', caption: 'server', value: 'server', scope: 1},
  {meta: 'keyword', caption: 'sets', value: 'sets', scope: 1},
  {meta: 'keyword', caption: 'shared', value: 'shared', scope: 1},
  {meta: 'keyword', caption: 'shiftleft', value: 'shiftleft', scope: 1},
  {meta: 'keyword', caption: 'shiftright', value: 'shiftright', scope: 1},
  {meta: 'keyword', caption: 'shiftrightunsigned', value: 'shiftrightunsigned', scope: 1},
  {meta: 'keyword', caption: 'show', value: 'show', scope: 1},
  {meta: 'keyword', caption: 'show_database', value: 'show_database', scope: 1},
  {meta: 'keyword', caption: 'sign', value: 'sign', scope: 1},
  {meta: 'keyword', caption: 'sin', value: 'sin', scope: 1},
  {meta: 'keyword', caption: 'size', value: 'size', scope: 1},
  {meta: 'keyword', caption: 'skewed', value: 'skewed', scope: 1},
  {meta: 'keyword', caption: 'smallint', value: 'smallint', scope: 1},
  {meta: 'keyword', caption: 'snapshot', value: 'snapshot', scope: 1},
  {meta: 'keyword', caption: 'sort', value: 'sort', scope: 1},
  {meta: 'keyword', caption: 'sorted', value: 'sorted', scope: 1},
  {meta: 'keyword', caption: 'sort_array', value: 'sort_array', scope: 1},
  {meta: 'keyword', caption: 'soundex', value: 'soundex', scope: 1},
  {meta: 'keyword', caption: 'space', value: 'space', scope: 1},
  {meta: 'keyword', caption: 'split', value: 'split', scope: 1},
  {meta: 'keyword', caption: 'sqrt', value: 'sqrt', scope: 1},
  {meta: 'keyword', caption: 'ssl', value: 'ssl', scope: 1},
  {meta: 'keyword', caption: 'statistics', value: 'statistics', scope: 1},
  {meta: 'keyword', caption: 'stack', value: 'stack', scope: 1},
  {meta: 'keyword', caption: 'start', value: 'start', scope: 1},
  {meta: 'keyword', caption: 'std', value: 'std', scope: 1},
  {meta: 'keyword', caption: 'stddev', value: 'stddev', scope: 1},
  {meta: 'keyword', caption: 'stddev_pop', value: 'stddev_pop', scope: 1},
  {meta: 'keyword', caption: 'stddev_samp', value: 'stddev_samp', scope: 1},
  {meta: 'keyword', caption: 'stored', value: 'stored', scope: 1},
  {meta: 'keyword', caption: 'str_to_map', value: 'str_to_map', scope: 1},
  {meta: 'keyword', caption: 'streamtable', value: 'streamtable', scope: 1},
  {meta: 'keyword', caption: 'string', value: 'string', scope: 1},
  {meta: 'keyword', caption: 'struct', value: 'struct', scope: 1},
  {meta: 'keyword', caption: 'substr', value: 'substr', scope: 1},
  {meta: 'keyword', caption: 'substring', value: 'substring', scope: 1},
  {meta: 'keyword', caption: 'summary', value: 'summary', scope: 1},
  {meta: 'keyword', caption: 'tan', value: 'tan', scope: 1},
  {meta: 'keyword', caption: 'table', value: 'table', scope: 1},
  {meta: 'keyword', caption: 'tables', value: 'tables', scope: 1},
  {meta: 'keyword', caption: 'tablesample', value: 'tablesample', scope: 1},
  {meta: 'keyword', caption: 'tblproperties', value: 'tblproperties', scope: 1},
  {meta: 'keyword', caption: 'temporary', value: 'temporary', scope: 1},
  {meta: 'keyword', caption: 'terminated', value: 'terminated', scope: 1},
  {meta: 'keyword', caption: 'then', value: 'then', scope: 1},
  {meta: 'keyword', caption: 'time', value: 'time', scope: 1},
  {meta: 'keyword', caption: 'timestamptz', value: 'timestamptz', scope: 1},
  {meta: 'keyword', caption: 'tinyint', value: 'tinyint', scope: 1},
  {meta: 'keyword', caption: 'to', value: 'to', scope: 1},
  {meta: 'keyword', caption: 'to_date', value: 'to_date', scope: 1},
  {meta: 'keyword', caption: 'to_unix_timestamp', value: 'to_unix_timestamp', scope: 1},
  {meta: 'keyword', caption: 'to_utc_timestamp', value: 'to_utc_timestamp', scope: 1},
  {meta: 'keyword', caption: 'touch', value: 'touch', scope: 1},
  {meta: 'keyword', caption: 'transaction', value: 'transaction', scope: 1},
  {meta: 'keyword', caption: 'transactions', value: 'transactions', scope: 1},
  {meta: 'keyword', caption: 'transform', value: 'transform', scope: 1},
  {meta: 'keyword', caption: 'translate', value: 'translate', scope: 1},
  {meta: 'keyword', caption: 'trigger', value: 'trigger', scope: 1},
  {meta: 'keyword', caption: 'trim', value: 'trim', scope: 1},
  {meta: 'keyword', caption: 'true', value: 'true', scope: 1},
  {meta: 'keyword', caption: 'trunc', value: 'trunc', scope: 1},
  {meta: 'keyword', caption: 'truncate', value: 'truncate', scope: 1},
  {meta: 'keyword', caption: 'unbase64', value: 'unbase64', scope: 1},
  {meta: 'keyword', caption: 'unarchive', value: 'unarchive', scope: 1},
  {meta: 'keyword', caption: 'unbounded', value: 'unbounded', scope: 1},
  {meta: 'keyword', caption: 'undo', value: 'undo', scope: 1},
  {meta: 'keyword', caption: 'unhex', value: 'unhex', scope: 1},
  {meta: 'keyword', caption: 'uniontype', value: 'uniontype', scope: 1},
  {meta: 'keyword', caption: 'uniquejoin', value: 'uniquejoin', scope: 1},
  {meta: 'keyword', caption: 'unix_timestamp', value: 'unix_timestamp', scope: 1},
  {meta: 'keyword', caption: 'unlock', value: 'unlock', scope: 1},
  {meta: 'keyword', caption: 'unset', value: 'unset', scope: 1},
  {meta: 'keyword', caption: 'unsigned', value: 'unsigned', scope: 1},
  {meta: 'keyword', caption: 'uri', value: 'uri', scope: 1},
  {meta: 'keyword', caption: 'upper', value: 'upper', scope: 1},
  {meta: 'keyword', caption: 'use', value: 'use', scope: 1},
  {meta: 'keyword', caption: 'user', value: 'user', scope: 1},
  {meta: 'keyword', caption: 'using', value: 'using', scope: 1},
  {meta: 'keyword', caption: 'utc', value: 'utc', scope: 1},
  {meta: 'keyword', caption: 'utctimestamp', value: 'utctimestamp', scope: 1},
  {meta: 'keyword', caption: 'utctimestamp', value: 'utctimestamp', scope: 1},
  {meta: 'keyword', caption: 'validate', value: 'validate', scope: 1},
  {meta: 'keyword', caption: 'values', value: 'values', scope: 1},
  {meta: 'keyword', caption: 'valuetype', value: 'valuetype', scope: 1},
  {meta: 'keyword', caption: 'var_pop', value: 'var_pop', scope: 1},
  {meta: 'keyword', caption: 'var_samp', value: 'var_samp', scope: 1},
  {meta: 'keyword', caption: 'varchar', value: 'varchar', scope: 1},
  {meta: 'keyword', caption: 'variance', value: 'variance', scope: 1},
  {meta: 'keyword', caption: 'vectorization', value: 'vectorization', scope: 1},
  {meta: 'keyword', caption: 'view', value: 'view', scope: 1},
  {meta: 'keyword', caption: 'views', value: 'views', scope: 1},
  {meta: 'keyword', caption: 'week', value: 'week', scope: 1},
  {meta: 'keyword', caption: 'weeks', value: 'weeks', scope: 1},
  {meta: 'keyword', caption: 'weekofyear', value: 'weekofyear', scope: 1},
  {meta: 'keyword', caption: 'while', value: 'while', scope: 1},
  {meta: 'keyword', caption: 'window', value: 'window', scope: 1},
  {meta: 'keyword', caption: 'windowingtablefunction', value: 'windowingtablefunction', scope: 1},
  {meta: 'keyword', caption: 'with', value: 'with', scope: 1},
  {meta: 'keyword', caption: 'work', value: 'work', scope: 1},
  {meta: 'keyword', caption: 'write', value: 'write', scope: 1},
  {meta: 'keyword', caption: 'xpath', value: 'xpath', scope: 1},
  {meta: 'keyword', caption: 'xpath_boolean', value: 'xpath_boolean', scope: 1},
  {meta: 'keyword', caption: 'xpath_double', value: 'xpath_double', scope: 1},
  {meta: 'keyword', caption: 'xpath_float', value: 'xpath_float', scope: 1},
  {meta: 'keyword', caption: 'xpath_int', value: 'xpath_int', scope: 1},
  {meta: 'keyword', caption: 'xpath_long', value: 'xpath_long', scope: 1},
  {meta: 'keyword', caption: 'xpath_number', value: 'xpath_number', scope: 1},
  {meta: 'keyword', caption: 'xpath_short', value: 'xpath_short', scope: 1},
  {meta: 'keyword', caption: 'xpath_string', value: 'xpath_string', scope: 1},
  {meta: 'keyword', caption: 'year', value: 'year', scope: 1},
  {meta: 'keyword', caption: 'years', value: 'years', scope: 1},
  {meta: 'keyword', caption: 'zone', value: 'zone', scope: 1}
]

export const assignTypes = [{value: 'user', label: 'user'}, {value: 'group', label: 'group'}]
export const sourceTypes = getFullMapping({
  NEW: 'undefined',
  HIVE: process.env.NODE_ENV === 'development' ? 11 : 9,
  RDBMS: 16,
  KAFKA: 1,
  RDBMS2: 8,
  // for newten
  // CSV: 11,
  SETTING: 'setting'
})

export const sourceNameMapping = {
  // for newten
  // HIVE: 'Hive',
  RDBMS: 'RDBMS',
  KAFKA: 'Kafka',
  RDBMS2: 'RDBMS',
  CSV: 'CSV',
  // should remove
  HIVE: 'Hive'
}

export const pageSizeMapping = {
  TABLE_TREE: 99,
  SEGMENT_CHART: 50
}

export const speedProjectTypes = [
  'AUTO_MAINTAIN'
]

export { projectCfgs } from './projectCfgs'
