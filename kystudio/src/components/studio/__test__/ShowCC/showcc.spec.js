import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import ShowCC from '../../StudioModel/ShowCC/showcc.vue'
import showccStore from '../../StudioModel/ShowCC/store'
import Vuex from 'vuex'

jest.useFakeTimers()

const store = new Vuex.Store({
  modules: {
    'ShowCCDialogModal': showccStore
  }
})

const wrapper = shallowMount(ShowCC, {
  store,
  localVue
})

describe('Component ShowCC', () => {
  it('init', async () => {
    wrapper.vm.$store.dispatch('ShowCCDialogModal/CALL_MODAL', {ccDetail: {columnName: 'CC1', datatype: 'verchar'}})
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.$store.state.ShowCCDialogModal.isShow).toBeTruthy()
  })
  it('computed', () => {
    expect(wrapper.vm.ccInfoData).toEqual([{'ccKey': 'Column Name', 'ccVal': 'CC1'}, {'ccKey': 'Return Type', 'ccVal': 'verchar'}])
  })
  it('methods', () => {
    wrapper.vm.closeModal(true)
    jest.runAllTimers()
    expect(wrapper.vm.$store.state.ShowCCDialogModal.isShow).toBeFalsy()
    expect(wrapper.vm.$store.state.ShowCCDialogModal.form).toEqual({'ccDetail': ''})
    jest.clearAllTimers()
  })
})
