import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import AddCC from '../../StudioModel/AddCCModal/addcc.vue'
import ccAddModalStore, {types} from '../../StudioModel/AddCCModal/store'
import Vuex from 'vuex'

jest.useFakeTimers()

const mockApi = {
  mockLoadModelInfo: jest.fn().mockResolvedValue(true)
}

ccAddModalStore.state = {
  isShow: false,
  callback: null,
  form: {
    modelInstance: null,
    currentCCForm: null
  },
  editCC: false
}

const store = new Vuex.Store({
  mutations: {
    'LOAD_MODEL_INFO': mockApi.mockLoadModelInfo
  },
  modules: {
    'CCAddModal': ccAddModalStore
  },
  getters: {
    currentSelectedProject () {
      return 'Kyligence'
    }
  }
})

const wrapper = shallowMount(AddCC, {
  store,
  localVue
})

describe('Component AddCC', () => {
  it('methods', () => {
    wrapper.vm.saveCC()
    jest.runAllTimers()
    expect(wrapper.vm.btnLoading).toBeFalsy()
    expect(wrapper.vm.$store.state.CCAddModal.isShow).toBeFalsy()
    expect(wrapper.vm.$store.state.CCAddModal.form).toEqual({'currentCCForm': null, 'modelInstance': null})

    wrapper.vm.saveCCError()
    expect(wrapper.vm.btnLoading).toBeFalsy()

    wrapper.vm.$refs = {
      ccForm: {
        $emit: jest.fn()
      }
    }

    wrapper.vm.submit()
    expect(wrapper.vm.btnLoading).toBeTruthy()
    expect(wrapper.vm.$refs.ccForm.$emit).toBeCalledWith('addCC')

    wrapper.vm.resetLoading()
    expect(wrapper.vm.btnLoading).toBeFalsy()

    wrapper.vm.$store.state.CCAddModal.callback = jest.fn()
    wrapper.vm.closeModal(true)
    jest.runAllTimers(200)
    expect(wrapper.vm.$store.state.CCAddModal.callback).toBeCalledWith(true)

    jest.clearAllTimers()
  })
  it('store', async () => {
    let state1 = {
      callback: null,
      form: {
        modelInstance: {},
        currentCCForm: { name: 'cc' }
      },
      editCC: true
    }
    ccAddModalStore.mutations[types.SET_MODAL_FORM](ccAddModalStore.state, state1)
    expect(ccAddModalStore.state.editCC).toBeTruthy()
    expect(ccAddModalStore.state.form).toEqual({'currentCCForm': undefined, 'modelInstance': undefined})

    ccAddModalStore.mutations[types.SHOW_MODAL](ccAddModalStore.state)
    expect(ccAddModalStore.state.isShow).toBeTruthy()

    ccAddModalStore.actions[types.CALL_MODAL]({commit: (name, payloads) => ccAddModalStore.mutations[name](ccAddModalStore.state, payloads)}, {modelInstance: {}, ccForm: {}, editCC: false})
    ccAddModalStore.state.callback && ccAddModalStore.state.callback()
    expect(ccAddModalStore.state.isShow).toBeTruthy()

    ccAddModalStore.actions[types.CALL_MODAL]({commit: (name, payloads) => ccAddModalStore.mutations[name](ccAddModalStore.state, payloads)}, {modelInstance: {}})
    ccAddModalStore.state.callback && ccAddModalStore.state.callback()
    expect(ccAddModalStore.state.isShow).toBeTruthy()
  })
})
