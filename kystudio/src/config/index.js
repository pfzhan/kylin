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
    color: 'gray',
    message: 'This model has no check result'
  }
}
export const needLengthMeasureType = ['fixed_length', 'fixed_length_hex', 'int', 'integer']
export const permissions = {
  READ: {name: 'CUBE QUERY', value: 'READ', mask: 1},
  MANAGEMENT: {name: 'CUBE EDIT', value: 'MANAGEMENT', mask: 32},
  OPERATION: {name: 'CUBE OPERATION', value: 'OPERATION', mask: 64},
  ADMINISTRATION: {name: 'CUBE ADMIN', value: 'ADMINISTRATION', mask: 16}
}
export const engineType = [
  {name: 'MapReduce', value: 2},
  {name: 'Spark (Beta)', value: 4}
]
export const SystemPwdRegex = /^(?=.*\d)(?=.*[a-zA-Z])(?=.*[~!@#$%^&*(){}|:"<>?[\];',./`]).{8,}$/
export const NamedRegex = /^\w+$/
export const DatePartitionRule = ['date', 'timestamp', 'string', 'bigint', 'int', 'integer', 'varchar']
export const TimePartitionRule = ['time', 'timestamp', 'string', 'varchar']
export const IntegerType = ['bigint', 'int', 'integer']

