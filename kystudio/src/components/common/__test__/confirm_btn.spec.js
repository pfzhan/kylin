import { mount } from '@vue/test-utils'
import ConfirmBtn from '../confirm_btn.vue'
import { localVue } from '../../../../test/common/spec_common'

const wrapper = mount(ConfirmBtn, {
  localVue,
  propsData: {
    tips: '确定按钮 popover 提示'
  }
})

describe('Component ConfirmBtn', () => {
  it('init', () => {
    const dom = wrapper.find('.el-popover').find('p')
    const btns = wrapper.findAll('.el-button').wrappers
    expect(dom.text()).toBe('确定按钮 popover 提示')
    btns[0].trigger('click')
    expect(wrapper.vm.visible).toBeFalsy()
    btns[1].trigger('click')
    expect(wrapper.emitted().okFunc).toEqual([[]])
  })
  it('methods', () => {
    wrapper.vm.okFunc()
    expect(wrapper.emitted().okFunc).toEqual([[], []])
    expect(wrapper.vm._data).toEqual({'visible': false})
  })
})
