// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue'

import ElementUI from 'element-ui'
import store from './store'

import 'less/index.less'
import 'element-ui/lib/theme-default/index.css'
import 'smooth-scrollbar/dist/smooth-scrollbar.css'

import fullLayout from './components/layout/layout_full'
import router from './router'
import mock from '../mock'
import filter from './filter'
import confirmBtn from './components/common/confirm_btn'

import Icon from 'vue-awesome/components/Icon.vue'
import 'vue-awesome/icons'
if (process.env.NODE_ENV !== 'production') {
  mock()
}

Vue.component('icon', Icon)
Vue.component('confirm-btn', confirmBtn)

Vue.use(ElementUI)

/* eslint-disable no-new */
new Vue({
  el: '#app',
  router,
  store,
  mock,
  filter,
  template: '<fullLayout/>',
  components: { fullLayout }
})
