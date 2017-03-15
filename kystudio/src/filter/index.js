import Vue from 'vue'
Vue.filter('nullValFilter', function (value) {
  return value == null ? 'null' : value
})

Vue.filter('utcTime', function (value) {
  if (!value || value && /[^\d]/.test(value)) {
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
