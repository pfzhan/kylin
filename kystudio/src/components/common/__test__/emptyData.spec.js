import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import EmptyData from '../EmptyData/EmptyData.vue'

const wrapper = shallowMount(EmptyData, {
  localVue,
  propsData: {
    image: 'xxx.png',
    content: '无内容',
    size: '10'
  }
})

describe('Component emptyData', () => {
  it('init', async () => {
    expect(wrapper.find('.empty-data').classes()).toContain('empty-data-10')
    expect(wrapper.find('img').attributes().src).toBe('xxx.png')
    expect(wrapper.findAll('.center').at(1).find('div').text()).toBe('无内容')
    await wrapper.setProps({
      content: '<p>no data</p>'
    })
  })
  it('test computed', async () => {
    expect(wrapper.vm.emptyImageUrl).toBe('xxx.png')
    expect(wrapper.vm.emptyContent).toBe('<p>no data</p>')
    expect(wrapper.vm.emptyClass).toBe('empty-data-10')
    await wrapper.setProps({
      image: '',
      content: '',
      size: ''
    })
    // await wrapper.update()
    expect(wrapper.vm.emptyImageUrl).toBe('')
    expect(wrapper.vm.emptyContent).toBe('No Data')
    expect(wrapper.vm.emptyClass).toBe('empty-data-normal')
  })
})
