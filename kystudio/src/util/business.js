import { utcToConfigTimeZone, removeNameSpace, getNameSpaceTopName } from './index'
import { permissionsMaps, NamedRegex, DatePartitionRule, TimePartitionRule } from 'config/index'
import { MessageBox, Message } from 'kyligence-ui'
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
  if (res === 'cancel' || res === true || res === false) {
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
        errorcallback(responseData, -1, res && res.status || -1, '')
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
export function kapConfirm (content, para) {
  var dialogPara = para || {type: 'warning'}
  return MessageBox.confirm(content, window.kapVm.$t('kylinLang.common.tip'), dialogPara)
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

// utc时间格式转换为gmt格式
export function transToGmtTime (t, _vue) {
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
// 根据别名查询table的全名
export function getTableNameInfoByAlias (modelDesc, aliasName) {
  if (!modelDesc || !aliasName) {
    return null
  }
  if (removeNameSpace(modelDesc.fact_table) === aliasName) {
    return {
      database: getNameSpaceTopName(modelDesc.fact_table),
      tableName: removeNameSpace(modelDesc.fact_table)
    }
  }
  var lookupLen = modelDesc.lookups && modelDesc.lookups.length || 0
  for (var i = 0; i < lookupLen; i++) {
    var curLookup = modelDesc.lookups[i]
    if (curLookup.alias === aliasName) {
      return {
        database: getNameSpaceTopName(curLookup.table),
        tableName: removeNameSpace(curLookup.table)
      }
    }
  }
  return null
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
export function transToUTCMs (date) {
  date = new Date(date)
  var y = date.getFullYear()
  var m = date.getMonth()
  var d = date.getDate()
  var h = date.getHours()
  var M = date.getMinutes()
  var s = date.getSeconds()
  return Date.UTC(y, m, d, h, M, s)
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
