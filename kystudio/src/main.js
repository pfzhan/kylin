// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import '@babel/polyfill'
import './util/polyfill'
import Vue from 'vue'
import ElementUI from 'kyligence-ui'
import store from './store'
import { v4 as uuidv4 } from 'uuid'
// import 'intro.js/introjs.css'
// import {introJs} from 'intro.js'
import fullLayout from 'components/layout/layout_full'
import router from './router'
import filters from './filter'
import directive from './directive'
import { sync } from './util/vuex-router-sync'
// common module register
import confirmBtn from 'components/common/confirm_btn'
import iconBtn from 'components/common/icon_button'
import commonTip from 'components/common/common_tip'
import tab from 'components/common/tab'
import pager from 'components/common/pager'// 弃用
import kapPager from 'components/common/kap_pager'
import slider from 'components/common/slider'
import nodata from 'components/common/nodata'
import emptyData from 'components/common/EmptyData/EmptyData.vue'
import progressbar from 'components/common/progress'
import commonPopover from 'components/common/common_popover'
import fakeProgress from 'components/common/fake_progress'
import pagerSelect from 'components/common/pager_filter_select'
import kapEditor from 'components/common/kap_editor'
import kapLoading from 'components/common/kap_loading'
import editor from 'vue2-ace-editor'
import VueClipboard from 'vue-clipboard2'
import VueKonva from 'vue-konva'
// import draggable from 'vuedraggable'
import nprogress from 'nprogress'
import 'brace/mode/json'
import 'brace/mode/sql'
import 'brace/snippets/sql'
import 'brace/theme/chrome'
import 'brace/theme/monokai'
import 'brace/ext/language_tools'
import './assets/styles/index.less'

Vue.component('confirm-btn', confirmBtn)
Vue.component('common-tip', commonTip)
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
Vue.component('kap-editor', kapEditor)
Vue.component('kap-tab', tab)
Vue.component('kap-loading', kapLoading)
Vue.component('kap-empty-data', emptyData)
import { getQueryString, cacheSessionStorage, cacheLocalStorage } from './util'
import { pageRefTags } from 'config'
// Vue.component('draggable', draggable)
// Vue.component('introJs', introJs)
// var cmdArg = process.argv.splice(2) && process.argv.splice(2)[0] || ''

// 第三方参数控制
// cloud 模式下弹窗通知父窗口
Vue.prototype.__KY_DIALOG_OPEN_EVENT__ = () => {
  if (store.state.config.platform === 'cloud' || store.state.config.platform === 'iframe') {
    window.parent.postMessage('dialogOpen', '*')
  }
}
Vue.prototype.__KY_DIALOG_CLOSE_EVENT__ = () => {
  if (store.state.config.platform === 'cloud' || store.state.config.platform === 'iframe') {
    window.parent.postMessage('dialogClose', '*')
  }
}
var from = getQueryString('from')
var uimode = getQueryString('uimode')
var token = getQueryString('token')
store.state.config.platform = (from === 'cloud' || from === 'iframe' || uimode === 'nomenu') ? 'iframe' : ''
if (token) {
  Vue.http.headers.common['Authorization'] = token
}
var projectName = getQueryString('projectName') || getQueryString('project') // projectName 兼容老cloud的嵌套，新的请使用project
if (projectName) {
  cacheSessionStorage('projectName', projectName)
  cacheLocalStorage('projectName', projectName)
}

// 第三方参数控制
// end
// Vue.prototype.introJs = introJs
Vue.use(ElementUI, {
  closeOtherMessages: true,
  errorMessageDuration: 10000,
  errorMessageShowClose: true
})
Vue.use(VueClipboard)
Vue.use(VueKonva)
Vue.http.headers.common['Accept-Language'] = localStorage.getItem('kystudio_lang') === 'en' ? 'en' : 'cn'
Vue.http.options.xhr = { withCredentials: true }
const skipUpdateApiList = [
  'kylin/api/system/servers',
  'kylin/api/jobs',
  'kylin/api/jobs/waiting_jobs',
  'kylin/api/query/favorite_queries',
  'kylin/api/query/favorite_queries/size',
  'kylin/api/system/license/nodes',
  'kylin/api/system/license/capacity'
]
Vue.http.interceptors.push(function (request, next) {
  const isProgressVisiable = !request.headers.get('X-Progress-Invisiable')
  request.headers['Cache-Control'] = 'no-cache'
  request.headers['If-Modified-Since'] = '0'
  if (request.url.indexOf('kylin/api/j_spring_security_logout') >= 0) {
    request.headers.set('Accept', 'text/html')
  } else if (request.url.indexOf('acl/') >= 0 && request.method === 'PUT') {
    request.headers.set('Accept', 'application/vnd.apache.kylin-v4-public+json')
  } else {
    request.headers.set('Accept', 'application/vnd.apache.kylin-v4+json')
  }
  skipUpdateApiList.forEach((url) => {
    if (request.url.indexOf(url)) {
      request.headers.set('Auto', request.params.isAuto ? 'true' : 'false')
    }
  })
  if (store.state.config.platform === 'cloud' || store.state.config.platform === 'iframe') { // 嵌套ifrme的平台
    // 云上时，针对除 get 以外的接口都要带上自定义的 request id
    const methodsArr = ['post', 'put', 'delete']
    const curMethod = request && request.method ? request.method.toLocaleLowerCase() : ''
    if (methodsArr.includes(curMethod)) {
      request.headers.set('Kylin-Request-Id', uuidv4())
    }
    nprogress.done()
    window.parent.postMessage('requestStart', '*')
  } else {
    isProgressVisiable && nprogress.start()
  }
  next(function (response) {
    if (store.state.config.platform === 'cloud' || store.state.config.platform === 'iframe') {
      window.parent.postMessage('requestEnd', '*')
    } else {
      isProgressVisiable && nprogress.done()
    }
    if (response.status === 401 && router.history.current.name !== 'login') {
      for (let p in pageRefTags) {
        const pager = pageRefTags[p]
        if (localStorage.getItem(pager)) {
          localStorage.removeItem(pager)
        }
      }
      if (store.state.config.platform === 'iframe') {
        window.parent.postMessage('keLogout', '*')
      } else {
        localStorage.setItem('loginIn', false)
        router.replace({name: 'Login', params: { ignoreIntercept: true }})
      }
    }
  })
})

sync(store, router)
/* eslint-disable no-new */
window.kapVm = new Vue({
  el: '#app',
  router,
  store,
  directive,
  filters,
  // introJs,
  template: '<fullLayout/>',
  components: { fullLayout }
})
