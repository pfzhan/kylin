import { shallowMount } from '@vue/test-utils'
import Vuex from 'vuex'
import AboutKap from '../about_kap.vue'
import { localVue } from '../../../../test/common/spec_common'

const store = new Vuex.Store({
  state: {
    system: {
      serverAboutKap: {
        'ke.dates': '2020-01-12,2020-01-25'
      },
      kyAccount: 'account',
      statement: null
    }
  }
})
const wrapper = shallowMount(AboutKap, {
  store,
  localVue,
  propsData: {
    about: '关于'
  }
})

delete window.location;
Object.defineProperty(window, 'location', {
    value: {
        href: '',
        hash: ''
    }
});

describe('Component about_kap', () => {
  it('init data', async () => {
    expect(wrapper.name()).toBe('about_kap')
    expect(wrapper.vm.aboutKap).toBe('关于')
  })
  it('test computed function', async () => {
    expect(wrapper.vm.serverAboutKap).toEqual({ 'ke.dates': '2020-01-12,2020-01-25' })
    expect(wrapper.vm.kyAccount).toBe('account')
    expect(wrapper.vm.statement).toBeNull()
    expect(wrapper.vm.licenseRange).toBe('2020-01-12 To 2020-01-25')
    wrapper.vm.$store.state.system.serverAboutKap['ke.dates'] = ''
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.licenseRange).toBe('')
  })
  it('test methods', async () => {
    wrapper.vm.requestLicense()
    expect(location.href).toBe('api/system/license/info')
  })
})
