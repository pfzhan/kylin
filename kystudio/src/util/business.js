import { utcToConfigTimeZone, getQueryString } from './index'
import { permissionsMaps, NamedRegex, DatePartitionRule, TimePartitionRule } from 'config/index'
import { MessageBox, Message } from 'kyligence-ui'
import moment from 'moment-timezone'

// 成功回调入口
export function handleSuccess (res, callback, errorcallback) {
  var responseData = res && res.data || null
  if (responseData && responseData.code === '000') {
    if (typeof callback === 'function') {
      callback(responseData.data, responseData.code, responseData.msg)
    }
  } else {
    callback(responseData && responseData.data || null, responseData && responseData.code || '000', responseData && responseData.msg || '')
  }
}
// 失败回调入口
export function handleError (res, errorcallback) {
  var responseData = res && res.data || null
  if (!res || res === 'cancel' || res === true || res === false) {
    return
  }
  // 服务器超时和无response的情况
  if (res.status === 504 || !res.status) {
    if (typeof errorcallback === 'function') {
      errorcallback(responseData, -1, res && res.status || -1, '')
      return
    }
    window.kapVm.$store.state.config.errorMsgBox.isShow = true
    window.kapVm.$store.state.config.errorMsgBox.msg = res.message || window.kapVm.$t('kylinLang.common.notConnectServer')
    window.kapVm.$store.state.config.errorMsgBox.detail = responseData && responseData.stacktrace || res.stack || JSON.stringify(res)
  } else {
    var msg = responseData && responseData.msg || window.kapVm.$t('kylinLang.common.unknownError')
    if (typeof errorcallback !== 'function') {
      window.kapVm.$store.state.config.errorMsgBox.isShow = true
      window.kapVm.$store.state.config.errorMsgBox.msg = msg
      window.kapVm.$store.state.config.errorMsgBox.detail = responseData && responseData.stacktrace || JSON.stringify(res)
    } else {
      if (responseData && responseData.code) {
        errorcallback(responseData.data, responseData.code, res.status, responseData.msg)
      } else {
        errorcallback(responseData, -1, res && res.status || -1, res && res.msg || '')
      }
    }
  }
  window.kapVm.$store.state.config.showLoadingBox = false
}
// 全局命名校验
export function validateNamed (rule, value, callback) {
  if (!NamedRegex.test(value)) {
    callback(new Error(window.kapVm.$t('kylinLang.common.nameFormatValidTip')))
  } else {
    callback()
  }
}
// 通用loading方法
export function loadingBox () {
  return {
    show () {
      window.kapVm.$store.state.config.showLoadingBox = true
    },
    hide () {
      window.kapVm.$store.state.config.showLoadingBox = false
    },
    status () {
      return window.kapVm.$store.state.config.showLoadingBox
    }
  }
}
// 确认弹窗
export function kapConfirm (content, para, title) {
  var dialogTitle = title || window.kapVm.$t('kylinLang.common.tip')
  var dialogPara = para || {type: 'warning'}
  return MessageBox.confirm(content, dialogTitle, dialogPara)
}

export function kapWarn (content, para) {
  var dialogPara = para || {
    type: 'warning',
    showCancelButton: false
  }
  return MessageBox.confirm(content, window.kapVm.$t('kylinLang.common.tip'), dialogPara)
}
export function kapMessage (content, para) {
  var messagePara = Object.assign({
    type: 'success',
    message: content,
    duration: 3000,
    showClose: false
  }, para)
  Message(messagePara)
}
// 获取基本encoding
export function loadBaseEncodings (state) {
  var resultArr = []
  return {
    filterByColumnType: function (columnType) {
      if (state.encodingCache[columnType]) {
        return state[columnType]
      }
      var matchArr = []
      for (let i in state.encodingMatchs) {
        if (i.indexOf(columnType.replace(/\([\d,]+\)/g, '')) >= 0) {
          matchArr = state.encodingMatchs[i]
          break
        }
      }
      for (let i = 0, len = (matchArr && matchArr.length || 0); i < len; i++) {
        for (var k in state.encodings) {
          if (k === matchArr[i]) {
            var obj = {
              name: matchArr[i],
              version: state.encodings[k]
            }
            resultArr.push(obj)
          }
        }
      }
      return resultArr
    },
    baseEncoding () {
    },
    removeEncoding (name) {
      var arr = []
      for (var i = 0; i < resultArr.length; i++) {
        if (resultArr[i].name !== name) {
          arr.push(resultArr[i])
        }
      }
      return arr
    },
    addEncoding: function (name, version) {
      for (var i = 0; i < resultArr.length; i++) {
        if (resultArr[i].name === name && resultArr[i].version === version) {
          return resultArr
        }
      }
      resultArr.push({
        name: name,
        version: version
      })
      return resultArr
    },
    getEncodingMaxVersion: function (encodingName) {
      return state.encodings[encodingName] || 1
    }
  }
}

// kylin配置中抓取属性值
export function getProperty (name, kylinConfig) {
  var result = (new RegExp(name + '=(.*?)\\n')).exec(kylinConfig)
  return result && result[1] || ''
}

// utc时间格式转换为本地时区的gmt格式
export function transToGmtTime (t, _vue) {
  let d = moment(new Date(t))
  if (d) {
    return d.format().replace(/T/, ' ').replace(/([+-])(\d+):\d+$/, ' GMT$1$2').replace(/0(\d)$/, '$1')
  }
  return ''
}

export function getLocalTimezone () {
  var d = new Date()
  var localOffset = -d.getTimezoneOffset() / 60
  return 'GMT' + (localOffset > 0 ? '+' + localOffset : localOffset)
}
// utc时间格式转换为服务端时区的gmt格式
export function transToServerGmtTime (t, _vue) {
  var v = _vue || window.kapVm
  if (v) {
    return utcToConfigTimeZone(t, v.$store.state.system.timeZone)
  }
}
// utc时间格式转换为gmt格式(by ajax)
export function transToGmtTimeAfterAjax (t, timeZone, _vue) {
  var v = _vue || window.kapVm
  if (v) {
    return utcToConfigTimeZone(t, timeZone)
  }
}

// 测试当前用户在默认project下的权限
export function hasPermission (vue) {
  var curUser = vue.$store.state.user.currentUser
  var curUserAccess = vue.$store.state.user.currentUserAccess
  if (!curUser || !curUserAccess) {
    return null
  }
  var masks = []
  for (var i = 1; i < arguments.length; i++) {
    if (arguments[i]) {
      masks.push(permissionsMaps[arguments[i]])
    }
  }
  if (masks.indexOf(curUserAccess) >= 0) {
    return true
  }
  return false
}
// 监测当前用户在某个特定project下的权限
export function hasPermissionOfProjectAccess (vue, projectAccess) {
  var curUser = vue.$store.state.user.currentUser
  if (!curUser || !projectAccess) {
    return null
  }
  var masks = []
  for (var i = 2; i < arguments.length; i++) {
    if (arguments[i]) {
      masks.push(permissionsMaps[arguments[i]])
    }
  }
  if (masks.indexOf(projectAccess) >= 0) {
    return true
  }
  return false
}
// 检测当前用户是否有某种角色
export function hasRole (vue, roleName) {
  var haseRole = false
  var curUser = vue.$store.state.user.currentUser
  if (curUser && curUser.authorities) {
    curUser.authorities.forEach((auth, index) => {
      if (auth.authority === roleName) {
        haseRole = true
      }
    })
  }
  return haseRole
}

// 过滤空值
export function filterNullValInObj (needFilterObj) {
  var newObj
  if (typeof needFilterObj === 'string') {
    newObj = JSON.parse(needFilterObj)
  } else {
    newObj = Object.assign({}, newObj)
  }
  function filterData (data) {
    var obj = data
    for (var i in obj) {
      if (obj[i] === null) {
        delete obj[i]
      } else if (typeof obj[i] === 'object') {
        obj[i] = filterData(obj[i])
      }
    }
    return obj
  }
  return JSON.stringify(filterData(newObj))
}
export function toDoubleNumber (n) {
  n = n > 9 ? n : '0' + n
  return n
}
export function transToUtcTimeFormat (ms, isSlash) {
  var date = new Date(ms)
  var y = date.getUTCFullYear()
  var m = date.getUTCMonth()
  var d = date.getUTCDate()
  var h = date.getUTCHours()
  var M = date.getUTCMinutes()
  var s = date.getUTCSeconds()
  var timeFormat = '-'
  if (isSlash) {
    timeFormat = '/'
  }
  var result = y + timeFormat + toDoubleNumber(m + 1) + timeFormat + toDoubleNumber(d) + ' ' + toDoubleNumber(h) + ':' + toDoubleNumber(M) + ':' + toDoubleNumber(s)
  return result
}
export function transToUtcDateFormat (ms, isSlash) {
  var date = new Date(ms)
  var y = date.getUTCFullYear()
  var m = date.getUTCMonth()
  var d = date.getUTCDate()
  var dataFormat = '-'
  if (isSlash) {
    dataFormat = '/'
  }
  var result = y + dataFormat + toDoubleNumber(m + 1) + dataFormat + toDoubleNumber(d)
  return result
}
export function transToUTCMs (ms, _vue) {
  let v = _vue || window.kapVm
  let zone = v.$store.state.system.timeZone
  if (ms === undefined || !zone) { return ms }
  let date = new Date(ms)
  let y = date.getFullYear()
  let m = date.getMonth()
  let d = date.getDate()
  let h = date.getHours()
  let M = date.getMinutes()
  let s = date.getSeconds()
  let timeFormat = '-'
  let utcTime = +Date.UTC(y, m, d, h, M, s)
  let dateStr = y + timeFormat + toDoubleNumber(m + 1) + timeFormat + toDoubleNumber(d) + ' ' + toDoubleNumber(h) + ':' + toDoubleNumber(M) + ':' + toDoubleNumber(s)
  let momentObj = moment(dateStr).tz(zone)
  let offsetMinutes = momentObj._offset
  utcTime = utcTime - offsetMinutes * 60000
  return utcTime
}

export function msTransDate (ms, limitWeeks) {
  var dateType = ['weeks', 'days', 'hours', 'minutes']
  var weeks = ms / 604800000
  var days = ms / 86400000
  var hours = ms / 3600000
  var minutes = ms / 60000
  if (weeks >= 1 && weeks === Math.floor(weeks) && !limitWeeks) {
    return {
      type: dateType[0],
      value: weeks
    }
  }
  if (days >= 1 && days === Math.floor(days)) {
    return {
      type: dateType[1],
      value: days
    }
  }
  if (hours >= 1 && hours === Math.floor(hours)) {
    return {
      type: dateType[2],
      value: hours
    }
  }
  if (minutes >= 1 && minutes === Math.floor(minutes)) {
    return {
      type: dateType[3],
      value: minutes
    }
  }
  return {
    type: dateType[3],
    value: minutes
  }
}
// 将复制进入编辑器的多行sql按分号转换成数组，剔除换行符
export function filterMutileSqlsToOneLine (_sqls, splitChar) {
  var regComment = /\/\*[\w\W]*?\*\/|\s*--.*$/gm // 过滤sql语句中的注释
  _sqls = _sqls.replace(regComment, ' ')
  var sqls = _sqls.split(splitChar || ';')
  sqls = sqls.filter((s) => {
    return !!(s.replace(/\s*[\r\n]?\s*/g, ''))
  })
  sqls = sqls.map((s) => {
    var r = s.replace(/[\r\n]+(\s+)?/g, ' ')
    return r.trim()
  })
  if (sqls[sqls.length - 1] === ' ') {
    sqls.splice(length - 1, 1)
  }
  return sqls
}

export function isDatePartitionType (type) {
  return DatePartitionRule.some(rule => rule.test(type))
}

export function isTimePartitionType (type) {
  return TimePartitionRule.some(rule => rule.test(type))
}

export function getGmtDateFromUtcLike (value) {
  if (value !== undefined && value !== null) {
    return new Date(transToServerGmtTime(value).replace(/\s+GMT[+-]\d+$/, '').replace(/-/g, '/'))
  }
}

export function getStringLength (value) {
  let len = 0
  value.split('').forEach((v) => {
    if (/[\u4e00-\u9fa5]/.test(v)) {
      len += 2
    } else {
      len += 1
    }
  })
  return len
}

/**
 * 获取当前时间过去某个时间段的时间（此方法方便做时间范围的判断）
 * @param {dateTime} date 时间点
 * @param {number} m 相差的月份
 * @example 当前时间2020-2-28 00:00:00，往前推m = 1个月，得到2020-1-28 00:00:00
 */
export function getPrevTimeValue ({ date, m = 0 }) {
  let dt = new Date(date)
  let year = dt.getFullYear()
  let month = dt.getMonth() + 1
  const day = dt.getDate()
  const hour = dt.getHours()
  const minutes = dt.getMinutes()
  const seconds = dt.getSeconds()

  // 判断是否大于12，大于则减去相应的年份
  if (typeof m !== 'undefined' && m !== 0) {
    if (month - m <= 0) {
      let y = Math.ceil(m / 12)
      month = m > 12 ? month + (12 * y - m) : 12 - Math.abs(month - m)
      year -= y
    } else {
      month -= m
    }
  }

  return `${year}/${toDoubleNumber(month)}/${toDoubleNumber(day)} ${toDoubleNumber(hour)}:${toDoubleNumber(minutes)}:${toDoubleNumber(seconds)}`
}

function getPlainRouteObj (route) {
  return {
    name: route.name || '',
    query: route.query || {},
    params: route.params || {}
  }
}

export function postCloudUrlMessage (fromRoute, toRoute) {
  if (getQueryString('from') === 'cloud' || getQueryString('from') === 'iframe') {
    const from = getPlainRouteObj(fromRoute)
    const to = getPlainRouteObj(toRoute)
    const message = JSON.stringify({ from, to })
    window.parent.postMessage(`changeUrl:${message}`, '*')
  }
}

