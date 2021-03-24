import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import ParameterTree from '../parameter_tree.vue'

const wrapper = shallowMount(ParameterTree, {
  localVue,
  propsData: {
    measure: {
      function: {
        parameter: {
          next_parameter: {
            value: 'test'
          }
        }
      }
    }
  }
})

describe('Component ParameterTree', () => {
  it('computed', async () => {
    expect(wrapper.vm.nextParaList).toEqual(['test'])

    await wrapper.setProps({measure: {
      function: {
        parameter: {
          next_parameter: {
            value: 'test',
            next_parameter: {
              value: 'test1'
            }
          }
        }
      }
    }})
    expect(wrapper.vm.nextParaList).toEqual(['test', 'test1'])

    let list = []
    wrapper.vm.recursion(wrapper.vm.measure.function.parameter.next_parameter, list)
    expect(list).toEqual(["test", "test1"])
  })
})
