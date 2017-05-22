export function fromObjToArr (obj) {
  let arr = []
  for (let key of Object.keys(obj)) {
    arr.push({
      key: key,
      value: obj[key]
    })
  }
  return arr
}

export function fromArrToObj (arr) {
  let obj = {}
  for (let item of arr) {
    obj[item.key] = item.value
  }
  return obj
}

export function sampleGuid () {
  let randomNumber = ('' + Math.random()).replace(/\./, '')
  return (new Date()).getTime() + '_' + randomNumber
}

export function removeNameSpace (str) {
  if (str) {
    return str.replace(/([^.\s]+\.)+/, '')
  } else {
    return ''
  }
}

export function getNameSpaceTopName (str) {
  if (str) {
    return str.replace(/(\.[^.]+)+/, '')
  } else {
    return ''
  }
}

export function getNameSpace (str) {
  if (str) {
    return str.replace(/(\.[^.]+)$/, '')
  } else {
    return ''
  }
}

export function changeDataAxis (data) {
  var len = data && data.length || 0
  var newArr = []
  var sublen = data && data.length && data[0].length || 0
  for (var i = 0; i < sublen; i++) {
    var subArr = []
    for (var j = 0; j < len; j++) {
      subArr.push(data[j][i])
    }
    newArr.push(subArr)
  }
  return newArr
}

// 将对象数组按照某一个key的值生成对象
export function groupData (data, groupName) {
  var len = data && data.length || 0
  var obj = {}
  for (var k = 0; k < len; k++) {
    obj[data[k][groupName]] = obj[data[k][groupName]] || []
    obj[data[k][groupName]].push(data[k])
  }
  return obj
}

export function transDataForTree (data) {
}
// 从对象数组中找到某个符合key value 的对象的位置
export function indexOfObjWithSomeKey (objectArr, key, equalVal) {
  for (var i = 0; i < objectArr.length; i++) {
    var filterObj = objectArr[i]
    if (filterObj[key] === equalVal) {
      return i
    }
  }
  return -1
}
// 对象数组排序 （chrome 对象数组排序原生sort有bug）
export function objectArraySort (objArr, sequence, sortKey) {
  var objectArr = objectClone(objArr)
  var condition
  for (var i = 0; i < objectArr.length; i++) {
    for (var k = i + 1; k < objectArr.length; k++) {
      if (sequence) {
        condition = objectArr[i][sortKey] > objectArr[k][sortKey]
      } else {
        condition = objectArr[i][sortKey] <= objectArr[k][sortKey]
      }
      if (condition) {
        let temp = objectArr[i]
        objectArr[i] = objectArr[k]
        objectArr[k] = temp
      }
    }
  }
  return objectArr
}

export function objectClone (obj) {
  if (typeof obj !== 'object') {
    return obj
  }
  var s = {}
  if (!obj) {
    return obj
  }
  if (obj.constructor === Array) {
    s = []
  }
  for (var i in obj) {
    s[i] = objectClone(obj[i])
  }
  return s
}
// 改变对象数组里对象的某个属性
export function changeObjectArrProperty (objectArr, key, val, newKey, newVal, _this) {
  var arr = objectClone(objectArr)
  let len = arr && arr.length || 0
  let setKey = ''
  let setVal = ''
  let vue = null
  if (key === '*') {
    setKey = val
    setVal = newKey
    vue = newVal
  } else {
    setKey = newKey
    setVal = newVal
    vue = _this
  }
  for (let i = 0; i < len; i++) {
    if (arr[i][key] === val || key === '*') {
      if (vue) {
        vue.$set(arr[i], setKey, setVal)
      } else {
        arr[i][setKey] = setVal
      }
    }
  }
}
// 获取对象数组对象属性符合条件的对象
export function filterObjectArray (objectArr, key, val) {
  objectArr = objectArr || []
  var resultArr = objectArr.filter((obj) => {
    return obj[key] === val
  })
  return resultArr
}
// 获取数组中指定元素的下一个元素
export function getNextValInArray (arr, current) {
  var arrLen = arr && arr.length || 0
  if (arrLen) {
    var index = arr.indexOf(current)
    if (index === -1) {
      return arr[0]
    }
    var next = index + 1 >= arrLen ? 0 : index + 1
    return arr[next]
  }
  return null
}
// 时间转换工具
import moment from 'moment'
// test console.log(utcToConfigTimeZome(1494399187389, 'GMT+8'))
export function utcToConfigTimeZome (item, zone, formatSet) {
  var timezone = zone || 'PST'
  var gmttimezone = ''
  if (item === '' || item === null || item === undefined) {
    return ''
  }
  var format = formatSet || 'YYYY-MM-DD HH:mm:ss'
  switch (timezone) {
    // convert PST to GMT
    case 'PST':
      gmttimezone = 'GMT-8'
      break
    default:
      gmttimezone = timezone
  }
  var localOffset = new Date().getTimezoneOffset()
  var convertedMillis = item
  var offset = gmttimezone.substr(4, 1)
  if (gmttimezone.indexOf('GMT+') !== -1) {
    convertedMillis = new Date(item).getTime() + offset * 60 * 60000 + localOffset * 60000
  } else if (gmttimezone.indexOf('GMT-') !== -1) {
    convertedMillis = new Date(item).getTime() - offset * 60 * 60000 + localOffset * 60000
  } else {
    // return PST by default
    timezone = 'PST'
    convertedMillis = new Date(item).getTime() - 8 * 60 * 60000 + localOffset * 60000
  }
  return moment(convertedMillis).format(format) + ' ' + timezone
}

