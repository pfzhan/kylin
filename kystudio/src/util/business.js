import { utcToConfigTimeZome, removeNameSpace, getNameSpaceTopName } from './index'
import { MessageBox, Message } from 'element-ui'
// 成功回调入口
export function handleSuccess (res, callback, errorcallback) {
  var responseData = res.data
  if (responseData && responseData.code === '000') {
    if (typeof callback === 'function') {
      callback(responseData.data, responseData.code, responseData.msg)
    }
  } else {
    callback(responseData.data, '000', responseData.msg)
  }
  // if (typeof errorcallback === 'function') {
  //   errorcallback(responseData.data, responseData.code)
  // }
}
// 失败回调入口
export function handleError (res, errorcallback) {
  var responseData = res.data
  if (typeof errorcallback !== 'function' && responseData.msg) {
    Message.error(responseData.msg || window.kapVm.$t('kylinLang.common.unknownError'))
  }
  if (responseData && responseData.code) {
    if (typeof errorcallback === 'function') {
      errorcallback(responseData.data, responseData.code, res.status, responseData.msg)
      return
    }
  } else {
    if (typeof errorcallback === 'function') {
      errorcallback(res.data, -1, res.status, '')
    }
  }
}

// 确认弹窗

export function kapConfirm (content, para) {
  var dialogPara = para || {}
  // console.log(Message, 32323)
  return MessageBox.confirm(content, window.kapVm.$t('kylinLang.common.tip'), dialogPara)
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
        if (i.indexOf(columnType.replace(/\(\d+\)/g, '')) >= 0) {
          matchArr = state.encodingMatchs[i]
          break
        }
      }
      for (let i = 0, len = matchArr && matchArr.length || 0; i < len; i++) {
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
export function transToGmtTime (t, vue) {
  return utcToConfigTimeZome(t, vue.$store.state.system.timeZone)
}

// 检测是否有project的某种权限
export function hasPermission (vue, projectId) {
  var entity = vue.$store.state.project.projectAccess[projectId]
  var curUser = vue.$store.state.user.currentUser
  if (!curUser) {
    return curUser
  }
  var hasPermission = false
  var masks = []
  for (var i = 2; i < arguments.length; i++) {
    if (arguments[i]) {
      masks.push(arguments[i])
    }
  }
  if (entity) {
    entity.forEach((acessEntity, index) => {
      if (masks.indexOf(acessEntity.permission.mask) !== -1) {
        if ((curUser.username === acessEntity.sid.principal)) {
          hasPermission = true
        }
      }
    })
  }
  return hasPermission
}
// 检测是否有cube的某种权限
export function hasPermissionOfCube (vue, cubeId) {
  var entity = vue.$store.state.cube.cubeAccess[cubeId]
  var curUser = vue.$store.state.user.currentUser
  if (!curUser) {
    return curUser
  }
  var hasPermission = false
  var masks = []
  for (var i = 2; i < arguments.length; i++) {
    if (arguments[i]) {
      masks.push(arguments[i])
    }
  }
  if (entity) {
    entity.forEach((acessEntity, index) => {
      if (masks.indexOf(acessEntity.permission.mask) !== -1) {
        if ((curUser.username === acessEntity.sid.principal)) {
          hasPermission = true
        }
      }
    })
  }
  return hasPermission
}
// 检测当前用户是否有某种角色
export function hasRole (vue, roleName) {
  var haseRole = false
  var curUser = vue.$store.state.user.currentUser
  if (curUser) {
    curUser.authorities.forEach((auth, index) => {
      if (auth.authority === roleName) {
        haseRole = true
      }
    })
  }
  return haseRole
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

