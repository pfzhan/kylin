import Vue from 'vue'
import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import Vuex from 'vuex'
import VueResource from 'vue-resource'
import changeLang from '../change_lang.vue'
import * as util from '../../../util/index'

localVue.use(VueResource)
// Vue.use(Vuex)
Vue.use(VueResource)

const store = new Vuex.Store({
  state: {
    system: {
      lang: 'en',
      messageDirectives: []
    }
  }
})
let mockFn = jest.spyOn(util, 'getQueryString').mockReturnValue('')
let mockEventBus = jest.fn().mockImplementation((type, callback) => {
  callback('en')
})

let wrapper = null

const factory = (mocks, language) => {
  // global.navigator.language = language || 'zh-cn'
  Object.defineProperty(global.navigator, 'language', {
    writable: true,
    value: language
  })
  return shallowMount(changeLang, {
    store,
    localVue,
    mocks: {
      // navigator: navigator,
      $_bus: {
        $on: mockEventBus
      },
      ...mocks
    }
  })
}

describe('Component change_lang', () => {
  it('init', () => {
    wrapper = factory({}, 'zh-cn')
    expect(wrapper.vm.defaultLang).toBe('zh-cn')
    localStorage.setItem('kystudio_lang', 'zh-cn')
    wrapper.setData({ lang: localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : wrapper.vm.defaultLang })
    expect(wrapper.vm.lang).toBe('zh-cn')
    wrapper.destroy()
  })
  it('created', () => {
    wrapper = factory({}, 'zh-cn')
    // expect(mockFn).toBeCalled()
    changeLang.created.call(wrapper.vm)
    expect(mockFn).toBeCalled()
    expect(mockFn).toHaveBeenCalledWith('lang')
    wrapper.destroy()
  })
  it('method changeLang', () => {
    wrapper = factory({}, 'zh-cn')
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
    wrapper.destroy()
  })
  it('no language', () => {
    wrapper = factory({}, '')
    expect(wrapper.vm.defaultLang).toBe('en')
    expect(mockEventBus).toBeCalled()
    expect(wrapper.vm.lang).toBe('en')
    wrapper.destroy()
  })
  it('api query', () => {
    mockFn = jest.spyOn(util, 'getQueryString').mockReturnValue('en')
    wrapper = factory({}, '')
    expect(wrapper.vm.lang).toBe('en')
    wrapper.destroy()
  })
  it('language message', () => {
    store.state.system.messageDirectives = [{action: 'changeLang', params: 'en'}]
    wrapper = factory({}, 'zh-cn')
    expect(wrapper.vm.lang).toBe('en')
    wrapper.destroy()
  })
})
