// 全局配置

let apiUrl
let baseUrl
let regexApiUrl

let pageCount = 9

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
  pageCount
}
export const modelHealthStatus = {
  'RUNNING': {
    icon: '',
    color: '',
    message: 'Job Running'
  },
  'GOOD': {
    icon: 'check-circle-o',
    color: 'green',
    message: 'Good Health'
  },
  'WARN': {
    icon: 'exclamation-circle',
    color: 'yellow',
    message: ''
  },
  'BAD': {
    icon: 'times-circle',
    color: 'red',
    message: ''
  },
  'TERRIBLE': {
    icon: 'times-circle',
    color: 'red',
    message: ''
  },
  'NONE': {
    icon: 'question-circle',
    color: '#6c707e',
    message: 'This model has no check result'
  },
  'ERROR': {
    icon: 'circle-o-notch',
    color: 'red',
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
export const DatePartitionRule = ['date', 'timestamp', 'string', 'bigint', 'int', 'integer', 'varchar']
export const TimePartitionRule = ['long', 'bigint', 'int', 'short', 'integer', 'tinyint', 'string', 'varchar', 'char']
export const IntegerType = ['bigint', 'int', 'integer', 'tinyint']
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

