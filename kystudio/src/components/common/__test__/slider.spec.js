import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import Slider from '../slider.vue'

const wrapper = shallowMount(Slider, {
  localVue,
  propsData: {
    label: 'slider',
    step: 10,
    showStops: false,
    max: null,
    min: null,
    show: true,
    hideCheckbox: true,
    range: 0,
    showRange: true
  }
})

describe('Component Slider', () => {
  it('methods', async () => {
    expect(wrapper.vm.formatTooltip(10)).toBe('10%')
    wrapper.vm.changeBarVal(50)
    expect(wrapper.emitted().changeBar).toEqual([[50]])
    expect(wrapper.vm.openCollectRange).toBeTruthy()
    expect(wrapper.vm.staticsRange).toBe(50)

    wrapper.vm.changeCollectRange()
    expect(wrapper.vm.staticsRange).toBe(100)
    expect(wrapper.emitted().changeBar).toEqual([[50], [100]])

    await wrapper.setData({openCollectRange: false})
    wrapper.vm.changeCollectRange()
    expect(wrapper.vm.staticsRange).toBe(0)
    expect(wrapper.emitted().changeBar).toEqual([[50], [100], [0]])

    await wrapper.setProps({show: false})
    expect(wrapper.vm.openCollectRange).toBeFalsy()
    expect(wrapper.vm.staticsRange).toBe(0)
    expect(wrapper.emitted().changeBar).toEqual([[50], [100], [0], [0]])

    await wrapper.setProps({step: null})
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.stepConfig).toBe(10)
  })
})
