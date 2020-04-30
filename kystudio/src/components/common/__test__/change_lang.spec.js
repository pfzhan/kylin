import Vue from 'vue'
import { shallow, createLocalVue } from 'vue-test-utils'
import Vuex from 'vuex'
import VueResource from 'vue-resource'
import changeLang from '../change_lang.vue'
import * as util from '../../../util/index'

const localVue = createLocalVue()
localVue.use(Vuex)
localVue.use(VueResource)
// Vue.use(Vuex)
Vue.use(VueResource)

const store = new Vuex.Store({
  state: {
    system: {
      lang: 'en'
    }
  }
})
let mockFn = jest.spyOn(util, 'getQueryString').mockReturnValue('')
const navigator = {
  language: 'zh-cn',
  browserLanguage: 'zh-cn'
}

global.navigator = navigator

let wrapper = null

describe('Component change_lang', () => {
  beforeEach(() => {
    wrapper = shallow(changeLang, {
      store,
      localVue,
      mocks: {
        navigator: navigator
      }
    })
  })
  afterEach(() => {
    wrapper.destroy()
  })
  it('init', () => {
    expect(wrapper.name()).toBe('changelang')
    expect(wrapper.vm.defaultLang).toBe('en')
    localStorage.setItem('kystudio_lang', 'zh-cn')
    wrapper.setData({ lang: localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : wrapper.vm.defaultLang })
    expect(wrapper.vm.lang).toBe('zh-cn')
  })
  it('created', () => {
    // expect(mockFn).toBeCalled()
    changeLang.created.call(wrapper.vm)
    expect(mockFn).toBeCalled()
    expect(mockFn).toHaveBeenCalledWith('lang')
  })
  it('method changeLang', () => {
    // 更改语言类型为英文时
    wrapper.vm.changeLang('en')
    expect(wrapper.vm.$store.state.system.lang).toBe('en')
    expect(wrapper.vm.$data.lang).toBe('en')
    expect(Vue.config.lang).toBe('en')
    expect(Vue.http.headers.common['Accept-Language']).toBe('en')
    expect(localStorage.getItem('kystudio_lang')).toBe('en')
    expect(document.documentElement.lang).toBe('en-us')
    // 更改语言类型为中文时
    wrapper.vm.changeLang('zh-cn')
    expect(wrapper.vm.$store.state.system.lang).toBe('zh-cn')
    expect(wrapper.vm.lang).toBe('zh-cn')
    expect(Vue.config.lang).toBe('zh-cn')
    expect(Vue.http.headers.common['Accept-Language']).toBe('cn')
    expect(localStorage.getItem('kystudio_lang')).toBe('zh-cn')
    expect(document.documentElement.lang).toBe('zh-cn')
  })
})
