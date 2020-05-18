import { mount } from 'vue-test-utils'
import EditableBlock from '../EditableBlock/EditableBlock'
import { localVue } from '../../../../test/common/spec_common'
import ElementUI from 'kyligence-ui'

localVue.use(ElementUI)
const wrapper = mount(EditableBlock, {
  localVue,
  propsData: {
    isEditable: true,
    headerContent: '内容',
    isKeepEditing: true,
    isEdited: false,
    isReset: true
  },
  scopedSlots: {
    default: '<p class="jest-test">test</p>'
  }
})

describe('Component EditableBlock', () => {
  it('init', () => {
    expect(wrapper.find('.block-foot').exists()).toBeTruthy()
    expect(wrapper.find('.block-header').exists()).toBeTruthy()
    expect(wrapper.find('.block-header span').text()).toBe('内容')
  })
  it('test funcs', async () => {
    expect(wrapper.vm.isEditing).toBe(true)
    expect(wrapper.vm.cancelText).toBe('Reset')
    wrapper.setProps({ isKeepEditing: false })
    await wrapper.update()
    expect(wrapper.vm.cancelText).toBe('Cancel')
    wrapper.find('.icon.el-icon-ksd-table_edit').trigger('click')
    expect(wrapper.vm.isEditing).toBe(true)
    wrapper.setProps({ isKeepEditing: true, isReset: true })
    await wrapper.update()
    expect(wrapper.find('.block-foot').exists()).toBeTruthy()
    expect(wrapper.vm.isReset).toBe(true)
    const btns = wrapper.findAll('.block-foot button')
    expect(btns.at(0).text()).toBe('Reset')
    btns.trigger('click')
    await wrapper.update()
    expect(wrapper.vm.isUserEditing).toBe(false)
    expect(wrapper.vm.isResetLoading).toBe(true)
    expect(wrapper.emitted().cancel[0].length).toBe(2)
    expect(wrapper.vm.isLoading).toBe(true)
    expect(wrapper.emitted().submit[0].length).toBe(2)
    wrapper.vm.handleError()
    expect(wrapper.vm.isLoading).toBe(false)
    expect(wrapper.vm.isResetLoading).toBe(false)
    wrapper.vm.handleSuccess()
    expect(wrapper.vm.isLoading).toBe(false)
    expect(wrapper.vm.isResetLoading).toBe(false)
    expect(wrapper.vm.isUserEditing).toBe(false)
  })
})
