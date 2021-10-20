import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import access from '../access.vue'
import columnAccess from '../access_sub/column_access'
import rowAccess from '../access_sub/row_access'
import tableAccess from '../access_sub/table_access'

const wrapper = shallowMount(access, {
  localVue,
  components: {
    columnAccess,
    rowAccess,
    tableAccess
  }
})

describe('Component Access', () => {
  it('init', async() => {
    wrapper.vm.$data.currentTab = 'table'
    await wrapper.vm.$nextTick()
    const table_access = wrapper.findComponent(tableAccess)
    expect(table_access.exists()).toBe(true)

    wrapper.vm.$data.currentTab = 'row'
    await wrapper.vm.$nextTick()
    const row_access = wrapper.findComponent(rowAccess)
    expect(row_access.exists()).toBe(true)

    wrapper.vm.$data.currentTab = 'column'
    await wrapper.vm.$nextTick()
    const column_access = wrapper.findComponent(columnAccess)
    expect(column_access.exists()).toBe(true)
  })
})
