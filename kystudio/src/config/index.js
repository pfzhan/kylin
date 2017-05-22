// 全局配置

let apiUrl
let baseUrl
let regexApiUrl

let pageCount = 12

if (process.env.NODE_ENV === 'development') {
  apiUrl = '/kylin/api/'
  baseUrl = '/kylin/'
  regexApiUrl = '\\/kylin\\/api\\/'
} else {
  apiUrl = '/kylin/api/'
  baseUrl = '/kylin/'
  regexApiUrl = '\\/kylin\\/api\\/'
}
const modelHealthStatus = {
  'GOOD': {
    icon: 'check-circle-o',
    color: 'green'
  },
  'WARN': {
    icon: 'exclamation-circle',
    color: 'yellow'
  },
  'BAD': {
    icon: 'times-circle',
    color: 'red'
  },
  'TERRIBLE': {
    icon: 'times-circle',
    color: 'red'
  },
  'NONE': {
    icon: 'question-circle',
    color: 'gray'
  }
}
export {
  // http请求
  apiUrl,
  baseUrl,
  regexApiUrl,
  // 分页
  pageCount,
  // model健康状态
  modelHealthStatus
}

