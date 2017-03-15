// 全局配置

let apiUrl
let baseUrl
let regexApiUrl

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
  apiUrl,
  baseUrl,
  regexApiUrl
}
