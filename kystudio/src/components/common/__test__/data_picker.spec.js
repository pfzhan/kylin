import { shallowMount, mount } from '@vue/test-utils'
import DatePicker from '../date_picker.vue'
import { localVue } from '../../../../test/common/spec_common'

const wrapper = mount(DatePicker, {
  localVue,
  propsData: {
    selectList: [],
    dateType: 'date'
  }
})

describe('Component DatePicker', () => {
  it('init', () => {
    const picker = wrapper.find('.el-date-editor input')
    picker.trigger('change', '2020-08-09')
    expect(wrapper.vm.timeSelect).toBe('')
    expect(wrapper.vm.selectList).toEqual([])
  })
  it('computed', async () => {
    expect(wrapper.vm.valueFormate).toBe('yyyy-MM-dd')
    await wrapper.setProps({ dateType: 'datetime' })
    // await wrapper.update()
    expect(wrapper.vm.valueFormate).toBe('yyyy-MM-dd HH:mm:SS')
  })
})
