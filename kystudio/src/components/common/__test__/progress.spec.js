import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import Progress from '../progress.vue'

const wrapper = shallowMount(Progress, {
  localVue,
  propsData: {
    percent: 50,
    status: 'PENDING'
  }
})

describe('Component Progress', () => {
  it('computed', async () => {
    expect(wrapper.vm.icon).toBe('')
    await wrapper.setProps({status: 'RUNNING'})
    expect(wrapper.vm.icon).toBe('')
    await wrapper.setProps({status: 'FINISHED'})
    expect(wrapper.vm.icon).toBe('el-icon-ksd-good_health')
    await wrapper.setProps({status: 'ERROR'})
    expect(wrapper.vm.icon).toBe('el-icon-ksd-error_01')
    await wrapper.setProps({status: 'DISCARDED'})
    expect(wrapper.vm.icon).toBe('el-icon-ksd-error_01')
    await wrapper.setProps({status: 'STOPPED'})
    expect(wrapper.vm.icon).toBe('el-icon-ksd-pause_02')

    await wrapper.setProps({status: 'PENDING'})
    expect(wrapper.vm.progressClass).toBe('pending')
    await wrapper.setProps({status: 'RUNNING'})
    expect(wrapper.vm.progressClass).toBe('running')
    await wrapper.setProps({status: 'FINISHED'})
    expect(wrapper.vm.progressClass).toBe('success')
    await wrapper.setProps({status: 'ERROR'})
    expect(wrapper.vm.progressClass).toBe('error')
    await wrapper.setProps({status: 'DISCARDED'})
    expect(wrapper.vm.progressClass).toBe('lose')
    await wrapper.setProps({status: 'STOPPED'})
    expect(wrapper.vm.progressClass).toBe('pause')
    await wrapper.setProps({status: ''})
    expect(wrapper.vm.progressClass).toBe()
  })
})
