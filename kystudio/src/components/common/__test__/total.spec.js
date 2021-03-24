import { mount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import Total from '../total.vue'
import Vuex from 'vuex'

const mockApi = {
  mockLoadProjectList: jest.fn().mockImplementation()
}

const store = new Vuex.Store({
  state: {
    project: {
      projectList: ['kyligence']
    }
  },
  actions: {
    'LOAD_PROJECT_LIST': mockApi.mockLoadProjectList
  }
})

const wrapper = mount(Total, {
  store,
  localVue,
  propsData: {
    name: 'Total'
  }
})

describe('Component Total', () => {
  it('init', () => {
    expect(mockApi.mockLoadProjectList).toBeCalled()
    expect(wrapper.find('.clearfix span').text()).toBe('Total Total')
  })
  it('computed', () => {
    expect(wrapper.vm.projectList).toEqual(['kyligence'])
  })
})
