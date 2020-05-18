import { createLocalVue } from 'vue-test-utils'
import VueI18n from 'vue-i18n'
import enKylinLocale from '../../src/locale/en'
import zhKylinLocale from '../../src/locale/zh-CN'
import Vuex from 'vuex'

const localVue = createLocalVue()
localVue.use(Vuex)
localVue.use(VueI18n)
localVue.locale('en', {kylinLang: enKylinLocale.default})
localVue.locale('zh-cn', {kylinLang: zhKylinLocale.default})

export {
  localVue
}
