import Vue from 'vue'
Vue.filter('nullValFilter', function (value) {
  return value == null ? 'null' : value
})
