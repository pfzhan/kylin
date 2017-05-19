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
    icon: 'green'
  },
  'WARN': {
    icon: 'yellow'
  },
  'BAD': {
    icon: 'red'
  },
  'TERRIBLE': {
    icon: 'red'
  },
  'NONE': {
    icon: 'question'
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

