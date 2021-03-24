import { mount, shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import commonTip from '../common_tip.vue'

const wrapper = shallowMount(commonTip, {
  localVue,
  propsData: {
    tips: '无内容'
  }
})

describe('Component commonTip', () => {
  it('default component commonTip', () => {
    expect(wrapper.exists()).toBe(true)
    expect(wrapper.find('el-tooltip-stub').exists()).toBeTruthy()
  })
  it('set tips', async () => {
    await wrapper.setProps({
      tips: '暂无数据'
    })
    // await wrapper.update()
    expect(wrapper.find('el-tooltip-stub').attributes().content).toBe('暂无数据')
  })
  it('set placement', async () => {
    await wrapper.setProps({
      placement: 'left'
    })
    // await wrapper.update()
    expect(wrapper.find('el-tooltip-stub').attributes().placement).toBe('left')
  })
  it('set content', async () => {
    await wrapper.setProps({
      content: '<p>hehehe</p>'
    })
    // await wrapper.update()
    expect(wrapper.find('el-tooltip-stub').find('div').html()).toBe('<div></div>')
  })
  it('set disabled', async () => {
    await wrapper.setProps({
      disabled: false
    })
    // await wrapper.update()
    expect(wrapper.vm.visible).toBe(false)
  })
})
