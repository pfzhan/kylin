// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue'

import ElementUI from 'element-ui'
import store from './store'

import 'less/index.less'
import 'element-ui/lib/theme-default/index.css'
import 'smooth-scrollbar/dist/smooth-scrollbar.css'
import 'nprogress/nprogress.css'
// import 'intro.js/introjs.css'
// import {introJs} from 'intro.js'
import fullLayout from 'components/layout/layout_full'
import router from './router'
import mock from '../mock'
import filters from './filter'
import directive from './directive'
// common module register
import confirmBtn from 'components/common/confirm_btn'
import iconBtn from 'components/common/icon_button'
import commonTip from 'components/common/common_tip'
import tree from 'components/common/tree'
import pager from 'components/common/pager'
import slider from 'components/common/slider'
import nodata from 'components/common/nodata'
import progressbar from 'components/common/progress'
import commonPopover from 'components/common/common_popover'
import fakeProgress from 'components/common/fake_progress'
import editor from 'vue2-ace-editor'
// import draggable from 'vuedraggable'
import nprogress from 'nprogress'

import Icon from 'vue-awesome/components/Icon.vue'
import 'vue-awesome/icons'
import 'brace/mode/json'
import 'brace/mode/sql'
import 'brace/theme/chrome'
// import Scrollbar from 'smooth-scrollbar'
import VueDND from 'awe-dnd'
Vue.use(VueDND)
Vue.component('icon', Icon)
Vue.component('confirm-btn', confirmBtn)
Vue.component('common-tip', commonTip)
Vue.component('tree', tree)
Vue.component('pager', pager)
Vue.component('kap-icon-button', iconBtn)
Vue.component('kap-nodata', nodata)
Vue.component('slider', slider)
Vue.component('kap-progress', progressbar)
Vue.component('editor', editor)
Vue.component('kap-common-popover', commonPopover)
Vue.component('fake_progress', fakeProgress)
// Vue.component('draggable', draggable)
// Vue.component('introJs', introJs)
// var cmdArg = process.argv.splice(2) && process.argv.splice(2)[0] || ''
if (process.env.NODE_ENV === 'development') {
  if (process.mock) {
    console.log('api proxy into mock')
    mock()
  }
}
// Vue.prototype.introJs = introJs
Vue.use(ElementUI)

Vue.http.headers.common['Accept-Language'] = localStorage.getItem('kystudio_lang') === 'en' ? 'en' : 'cn'
Vue.http.interceptors.push(function (request, next) {
  request.headers['Cache-Control'] = 'no-cache'
  request.headers['If-Modified-Since'] = '0'
  if (request.url.indexOf('kylin/api/j_spring_security_logout') >= 0) {
    request.headers.set('Accept', 'text/html')
  } else {
    request.headers.set('Accept', 'application/vnd.apache.kylin-v2+json')
  }
  nprogress.start()
  next(function (response) {
    nprogress.done()
    if (response.status === 401 && router.history.current.name !== 'login') {
      router.replace('/access/login')
    }
  })
})

router.beforeEach((to, from, next) => {
  if (to.matched && to.matched.length) {
    store.state.config.layoutConfig.gloalProjectSelectShow = to.name !== 'Dashboard'
    store.state.config.routerConfig.currentPathName = to.name
    // var scrollBar = Scrollbar.get(document.getElementById('scrollBox'))
    // scrollBar && scrollBar.setPosition(0, 0)
    next()
  } else {
    router.replace('/access/login')
  }
})
router.afterEach(route => {
  if (document.getElementById('scrollBox')) {
    document.getElementById('scrollBox').scrollTop = 0
  }
})

/* eslint-disable no-new */
window.kapVm = new Vue({
  el: '#app',
  router,
  store,
  mock,
  directive,
  filters,
  // introJs,
  template: '<fullLayout/>',
  components: { fullLayout }
})
