import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import ModelList from '../model_list.vue'

const wrapper = shallowMount(ModelList, {
  localVue,
  propsData: {
    modelList: ['model1', 'model2']
  },
  mocks: {
    $router: {
      push: jest.fn()
    }
  }
})

describe('Component ModelList', () => {
  it('init', () => {
    expect(wrapper.vm.modelItem).toEqual([{modelName: 'model1'}, {modelName: 'model2'}])
    expect(wrapper.vm.modelsTotal).toBe(2)
  })
  it('methods', () => {
    wrapper.vm.pageCurrentChange(2)
    expect(wrapper.vm.modelItem).toEqual([])
    wrapper.vm.gottoModel()
    expect(wrapper.vm.$router.push).toBeCalledWith('/studio/model')
  })
})
