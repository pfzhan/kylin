// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue'
import VueI18n from 'vue-i18n'
import ElementUI from 'element-ui'
import enLocale from 'element-ui/lib/locale/lang/en'
import zhLocale from 'element-ui/lib/locale/lang/zh-CN'
import enKylinLocale from 'locale/en'
import zhKylinLocale from 'locale/zh-CN'
import store from './store'
import 'element-ui/lib/theme-default/index.css'
import fullLayout from './components/layout/layout_full'
import router from './router'
import mock from '../mock'
import filter from './filter'
import Icon from 'vue-awesome/components/Icon.vue'
Vue.component('icon', Icon)
mock()
Vue.use(VueI18n)
Vue.use(ElementUI)

Vue.config.lang = localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : 'zh-cn'
enLocale.kylinLang = enKylinLocale.default
zhLocale.kylinLang = zhKylinLocale.default
Vue.locale('zh-cn', zhLocale)
Vue.locale('en', enLocale)
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
