import Vue from 'vue'
Vue.filter('nullValFilter', function (value) {
  return value == null ? 'null' : value
})

Vue.filter('utcTime', function (value) {
  if (/[^\d]/.test(value) || value === '') {
    return ''
  }
  var dateObj = new Date(value)
  var year = dateObj.getUTCFullYear()
  var month = (dateObj.getUTCMonth() + 1) < 10 ? '0' + (dateObj.getUTCMonth() + 1) : (dateObj.getUTCMonth() + 1)
  var date = dateObj.getUTCDate() < 10 ? '0' + dateObj.getUTCDate() : dateObj.getUTCDate()
  var hour = dateObj.getUTCHours() < 10 ? '0' + dateObj.getUTCHours() : dateObj.getUTCHours()
  var mins = dateObj.getUTCMinutes() < 10 ? '0' + dateObj.getUTCMinutes() : dateObj.getUTCMinutes()
  var seconds = dateObj.getUTCSeconds() < 10 ? '0' + dateObj.getUTCSeconds() : dateObj.getUTCSeconds()
  return year + '-' + month + '-' + date + ' ' + hour + ':' + mins + ':' + seconds
})

Vue.filter('utcTimeOrInt', function (value, isInt) {
  if (/[^\d]/.test(value) || value === '') {
    return ''
  }
  if (isInt) {
    return value
  }
  var dateObj = new Date(value)
  var year = dateObj.getUTCFullYear()
  var month = (dateObj.getUTCMonth() + 1) < 10 ? '0' + (dateObj.getUTCMonth() + 1) : (dateObj.getUTCMonth() + 1)
  var date = dateObj.getUTCDate() < 10 ? '0' + dateObj.getUTCDate() : dateObj.getUTCDate()
  var hour = dateObj.getUTCHours() < 10 ? '0' + dateObj.getUTCHours() : dateObj.getUTCHours()
  var mins = dateObj.getUTCMinutes() < 10 ? '0' + dateObj.getUTCMinutes() : dateObj.getUTCMinutes()
  var seconds = dateObj.getUTCSeconds() < 10 ? '0' + dateObj.getUTCSeconds() : dateObj.getUTCSeconds()
  return year + '-' + month + '-' + date + ' ' + hour + ':' + mins + ':' + seconds
})

Vue.filter('gmtTime', function (value) {
  if (/[^\d]/.test(value) || value === '') {
    return ''
  }
  var dateObj = new Date(value)
  var year = dateObj.getUTCFullYear()
  var month = (dateObj.getUTCMonth() + 1) < 10 ? '0' + (dateObj.getUTCMonth() + 1) : (dateObj.getUTCMonth() + 1)
  var date = dateObj.getUTCDate() < 10 ? '0' + dateObj.getUTCDate() : dateObj.getUTCDate()
  var hour = dateObj.getUTCHours() < 10 ? '0' + dateObj.getUTCHours() : dateObj.getUTCHours()
  var mins = dateObj.getUTCMinutes() < 10 ? '0' + dateObj.getUTCMinutes() : dateObj.getUTCMinutes()
  var seconds = dateObj.getUTCSeconds() < 10 ? '0' + dateObj.getUTCSeconds() : dateObj.getUTCSeconds()
  return year + '-' + month + '-' + date + ' ' + hour + ':' + mins + ':' + seconds
})

Vue.filter('timeFormatHasTimeZone', function (value) {
  if (/[^\d]/.test(value) || value === '') {
    return ''
  }

  var dateObj = new Date(value)
  var year = dateObj.getFullYear()
  var month = (dateObj.getMonth() + 1) < 10 ? '0' + (dateObj.getMonth() + 1) : (dateObj.getMonth() + 1)
  var date = dateObj.getDate() < 10 ? '0' + dateObj.getDate() : dateObj.getDate()
  var hour = dateObj.getHours() < 10 ? '0' + dateObj.getHours() : dateObj.getHours()
  var mins = dateObj.getMinutes() < 10 ? '0' + dateObj.getMinutes() : dateObj.getMinutes()
  var seconds = dateObj.getSeconds() < 10 ? '0' + dateObj.getSeconds() : dateObj.getSeconds()
  var gmtHours = -(dateObj.getTimezoneOffset() / 60)
  return year + '-' + month + '-' + date + ' ' + hour + ':' + mins + ':' + seconds + ' GMT' + (gmtHours >= 0 ? '+' + gmtHours : gmtHours)
})

Vue.filter('fixed', function (value, len) {
  var filterValue = value || ''
  var reg = new RegExp('^(\\d+?(?:\\.\\d{' + len + '})).*$')
  return ('' + filterValue).replace(reg, '$1')
})
/*
 *cut string with replaceChar, the double char has double length for cut
*/
Vue.filter('omit', function (value, len, replaceChar) {
  if (value) {
    if (len) {
      var cutIndex = 0
      value = value.replace(/./g, function (c) {
        if (/[\u4e00-\u9fa5]/.test(c)) {
          cutIndex += 2
        } else {
          cutIndex += 1
        }
        if (cutIndex > len) {
          return ''
        } else {
          return c
        }
      })
      if (cutIndex > len) {
        if (replaceChar) {
          return value + replaceChar
        }
      }
    }
  }
  return value
})

Vue.filter('number', function (value, fix) {
  if (isNaN(value)) {
    value = 0
  }
  return +value.toFixed(fix)
})

Vue.filter('readableNumber', function (value) {
  var b = value.toString()
  var len = b.length
  if (len <= 3) {
    return b
  }
  var r = len % 3
  return r > 0 ? b.slice(0, r) + ',' + b.slice(r, len).match(/\d{3}/g).join(',') : b.slice(r, len).match(/\d{3}/g).join(',')
})
// the time mins and seconds
Vue.filter('tofixedTimer', function (value, fix) {
  if (value > 60) {
    return (value / 60).toFixed(fix) + ' mins'
  } else {
    return value + ' seconds'
  }
})

Vue.filter('dataSize', function (data) {
  var size
  if (data / 1024 / 1024 / 1024 / 1024 >= 1) {
    size = (data / 1024 / 1024 / 1024 / 1024).toFixed(2) + ' TB'
  } else if (data / 1024 / 1024 / 1024 >= 1) {
    size = (data / 1024 / 1024 / 1024).toFixed(2) + ' GB'
  } else if (data / 1024 / 1024 >= 1) {
    size = (data / 1024 / 1024).toFixed(2) + ' MB'
  } else {
    size = (data / 1024).toFixed(2) + ' KB'
  }
  return size
})
Vue.filter('timeSize', function (data) {
  var size
  if (data / 1000 / 60 / 60 / 24 >= 1) {
    size = (data / 1000 / 60 / 60 / 24).toFixed(2) + ' days'
  } else if (data / 1000 / 60 / 60 >= 1) {
    size = (data / 1000 / 60 / 60).toFixed(2) + ' hours'
  } else if (data / 1000 / 60 >= 1) {
    size = (data / 1000 / 60).toFixed(2) + ' minutes'
  } else {
    return (data / 1000) + ' seconds'
  }
  return size
})

Vue.filter('filterArr', function (data, key, value, isFuzzy) {
  return data && data.filter((d) => {
    return !value || isFuzzy ? d[key].toLowerCase().indexOf(value.toLowerCase()) >= 0 : d[key] === value
  }) || []
})

Vue.filter('arrayToStr', function (data) {
  if (Array.isArray(data)) {
    return data.join(',')
  }
})
