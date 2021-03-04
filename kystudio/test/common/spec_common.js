import { createLocalVue } from '@vue/test-utils'
import VueI18n from 'vue-i18n'
import enKylinLocale from '../../src/locale/en'
import zhKylinLocale from '../../src/locale/zh-CN'
import Vuex from 'vuex'
import KyligenceUI from 'kyligence-ui'
import VueClipboard from 'vue-clipboard2'
import { createDirectives } from './directive'
import kapPager from 'components/common/kap_pager'
import emptyData from 'components/common/EmptyData/EmptyData.vue'
import kapNodata from 'components/common/nodata.vue'
import kapEditor from 'components/common/kap_editor'
import kapLoading from 'components/common/kap_loading'
import commonTip from 'components/common/common_tip'
import editor from 'vue2-ace-editor'
jest.mock('components/common/kap_editor', () => {
  return {
    setOption: jest.fn()
  }
})
jest.mock('vue2-ace-editor', ()=> {
  return {
    editor: {setOptions: jest.fn()}
  }
})

const mockEchartsEvent = {
  setOption: jest.fn(),
  dispose: jest.fn(),
  resize: jest.fn()
}
jest.mock('echarts', () => {
  return {
    init: () => {
      return {
        setOption: mockEchartsEvent.setOption,
        dispose: mockEchartsEvent.dispose,
        resize: mockEchartsEvent.resize
      }
    }
  }
})

const localVue = createLocalVue()
localVue.use(Vuex)
localVue.use(VueI18n)
localVue.use(KyligenceUI)
localVue.use(VueClipboard)
localVue.locale('en', {kylinLang: enKylinLocale.default})
localVue.locale('zh-cn', {kylinLang: zhKylinLocale.default})
localVue.component('kap-pager', kapPager)
localVue.component('kap-empty-data', emptyData)
localVue.component('kap-nodata', kapNodata)
localVue.component('editor', editor)
localVue.component('kap-editor', kapEditor)
localVue.component('kap-loading', kapLoading)
localVue.component('common-tip', commonTip)
createDirectives(localVue)

export {
  localVue,
  mockEchartsEvent
}
