import { mount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import Tab from '../tab.vue'
import Vuex from 'vuex'

const store = new Vuex.Store({
  state: {
    system: {
      lang: 'en'
    }
  }
})

const wrapper = mount(Tab, {
  store,
  localVue,
  propsData: {
    tabslist: [{name: 'query', icon: 'el-icon-ksd-healthy', i18n: false, closable: true, disabled: false, title: 'query', spin: ''}],
    isedit: false,
    active: 'query',
    type: 'card'
  }
})

describe('Compenont Tab', () => {
  it('computed', () => {
    expect(wrapper.vm.tabs).toEqual([{name: 'query', icon: 'el-icon-ksd-healthy', i18n: false, closable: true, disabled: false, title: 'query', spin: ''}])
    expect(wrapper.vm.activeName).toBe('query')
  })
  it('methods', async () => {
    wrapper.vm.handleClick({name: 'query', icon: 'el-icon-ksd-healthy', i18n: false, closable: true, disabled: false, title: 'query', spin: ''})
    expect(wrapper.emitted().clicktab).toEqual([['query']])

    wrapper.vm.handleTabsEdit('query', 'remove')
    expect(wrapper.emitted().removetab).toEqual([['query', '', '_close']])

    wrapper.vm.handleTabsEdit('query', 'add')
    expect(wrapper.emitted().removetab[1]).toEqual()

    await wrapper.setProps({active: 'query1'})
    expect(wrapper.vm.activeName).toBe('query1')
  })
})
