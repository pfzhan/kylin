// 全局配置

let apiUrl
let baseUrl

if (process.env.NODE_ENV === 'development') {
  apiUrl = '/kylin/api/'
  baseUrl = '/kylin/'
} else {
  apiUrl = '/kylin/api/'
  baseUrl = '/kylin/'
}

export {
  apiUrl,
  baseUrl
}
