// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue'

import ElementUI from 'kyligence-ui'
import store from './store'

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
import tab from 'components/common/tab'
import pager from 'components/common/pager'
import kapPager from 'components/common/kap_pager'// 弃用
import slider from 'components/common/slider'
import nodata from 'components/common/nodata'
import progressbar from 'components/common/progress'
import commonPopover from 'components/common/common_popover'
import fakeProgress from 'components/common/fake_progress'
import pagerSelect from 'components/common/pager_filter_select'
import kapEditor from 'components/common/kap_editor'
import kapValidateEditor from 'components/common/kap_validate_editor'
import kapLoading from 'components/common/kap_loading'
import editor from 'vue2-ace-editor'
import VueClipboard from 'vue-clipboard2'
// import draggable from 'vuedraggable'
import nprogress from 'nprogress'
import 'brace/mode/json'
import 'brace/mode/sql'
import 'brace/snippets/sql'
import 'brace/theme/chrome'
import 'brace/theme/monokai'
import 'brace/ext/language_tools'
import './assets/styles/index.less'
import VueDND from 'awe-dnd'
import * as types from './store/types'
Vue.use(VueDND)
Vue.component('confirm-btn', confirmBtn)
Vue.component('common-tip', commonTip)
Vue.component('tree', tree)
Vue.component('pager', pager) // 弃用
Vue.component('kap-pager', kapPager)
Vue.component('kap-icon-button', iconBtn)
Vue.component('kap-nodata', nodata)
Vue.component('slider', slider)
Vue.component('kap-progress', progressbar)
Vue.component('editor', editor)
Vue.component('kap-common-popover', commonPopover)
Vue.component('fake_progress', fakeProgress)
Vue.component('kap-filter-select', pagerSelect)
Vue.component('kap_editor', kapEditor)
Vue.component('kap-tab', tab)
Vue.component('kap-validate-editor', kapValidateEditor)
Vue.component('kap-loading', kapLoading)
import { menusData } from './config'
import { getQueryString, cacheSessionStorage, cacheLocalStorage } from './util'
// Vue.component('draggable', draggable)
// Vue.component('introJs', introJs)
// var cmdArg = process.argv.splice(2) && process.argv.splice(2)[0] || ''
if (process.env.NODE_ENV === 'development') {
  if (process.mock) {
    console.log('api proxy into mock')
    mock()
  }
}

// cloud 模式下弹窗通知父窗口
Vue.prototype.__KY_DIALOG_OPEN_EVENT__ = () => {
  if (store.state.config.platform === 'cloud') {
    window.parent.postMessage('dialogOpen', '*')
  }
}
Vue.prototype.__KY_DIALOG_CLOSE_EVENT__ = () => {
  if (store.state.config.platform === 'cloud') {
    window.parent.postMessage('dialogClose', '*')
  }
}
var from = getQueryString('from')
var token = getQueryString('token')
// var selectedProject = store.state.project.selected_project // 等api通了恢复
store.state.config.platform = from
if (from === 'cloud') {
  var projectName = getQueryString('projectName')
  cacheSessionStorage('projectName', projectName)
  cacheLocalStorage('projectName', projectName)
  Vue.http.headers.common['Authorization'] = token
}
// end
// Vue.prototype.introJs = introJs
Vue.use(ElementUI)
Vue.use(VueClipboard)
Vue.http.headers.common['Accept-Language'] = localStorage.getItem('kystudio_lang') === 'en' ? 'en' : 'cn'
Vue.http.options.xhr = { withCredentials: true }
Vue.http.interceptors.push(function (request, next) {
  request.headers['Cache-Control'] = 'no-cache'
  request.headers['If-Modified-Since'] = '0'
  if (request.url.indexOf('kylin/api/j_spring_security_logout') >= 0) {
    request.headers.set('Accept', 'text/html')
  } else {
    request.headers.set('Accept', 'application/vnd.apache.kylin-v2+json')
  }
  if (store.state.config.platform === 'cloud') {
    nprogress.done()
    window.parent.postMessage('requestStart', '*')
  } else {
    nprogress.start()
  }
  next(function (response) {
    if (store.state.config.platform === 'cloud') {
      window.parent.postMessage('requestEnd', '*')
    } else {
      nprogress.done()
    }
    if (response.status === 401 && router.history.current.name !== 'login') {
      router.replace('/access/login')
    }
  })
})
router.beforeEach((to, from, next) => {
  ElementUI.Message.closeAll()
  if (to.matched && to.matched.length) {
    store.state.config.layoutConfig.gloalProjectSelectShow = to.name !== 'Overview'
    // 确保在非点击菜单的路由跳转下还能够正确定位到指定的active name
    menusData.forEach((menu) => {
      if (menu.name.toLowerCase() === to.name.toLowerCase()) {
        store.state.config.routerConfig.currentPathName = menu.path
      }
    })
    // for newten
    // let configPromise = store.dispatch(types.GET_CONF)
    let authenticationPromise = store.dispatch(types.LOAD_AUTHENTICATION)
    let projectPromise = store.dispatch(types.LOAD_ALL_PROJECT)
    let rootPromise = Promise.all([
      // for newten
      // configPromise,
      authenticationPromise,
      projectPromise
    ])
    // 如果是从登陆过来的，所有信息都要重新获取
    if (from.name === 'Login' && (to.name !== 'access' && to.name !== 'Login')) {
      rootPromise.then(() => {
        // let configPromise = store.dispatch(types.GET_CONF, {
        //   projectName: selectedProject
        // })
        next()
        // configPromise.then(() => {  // 等api通了恢复
        //   next()
        // })
      })
    } else if (from.name !== 'access' && from.name !== 'Login' && to.name !== 'access' && to.name !== 'Login') {
      // 如果是非登录页过来的，内页之间的路由跳转的话，就需要判断是否已经拿过权限
      if (store.state.system.authentication === null && store.state.system.serverConfig === null) {
        rootPromise.then(() => {
          store.commit(types.SAVE_CURRENT_LOGIN_USER, { user: store.state.system.authentication.data })
          // let configPromise = store.dispatch(types.GET_CONF, { // 等api通了恢复
          //   projectName: selectedProject
          // })
          // configPromise.then(() => {
          //   next()
          // })
          next()
        }, (res) => {
          next()
        })
      } else {
        next()
      }
    } else {
      next()
    }
  } else {
    router.replace('/access/login')
  }
})
router.afterEach(route => {
  var scrollBoxDom = document.getElementById('scrollBox')
  if (scrollBoxDom) {
    scrollBoxDom.scrollTop = 0
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
