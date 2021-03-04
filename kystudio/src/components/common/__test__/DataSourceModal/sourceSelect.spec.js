import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import SourceSelect from '../../DataSourceModal/SourceSelect/SourceSelect'

const mockSourceTypes = {
  HIVE: 9,
  RDBMS: 16,
  KAFKA: 1,
  RDBMS2: 8,
  CSV: 13
}

const wrapper = shallowMount(SourceSelect, {
  localVue,
  propsData: {
    sourceType: 9
  },
  mocks: {
    sourceTypes: mockSourceTypes
  }
})

describe('Component SourceSelect', () => {
  it('init', () => {
    expect(wrapper.vm.sourceType).toBe(9)
    expect(wrapper.emitted().input).toEqual([[9]])
  })
  it('methods', () => {
    expect(wrapper.vm.getSourceClass([9])).toEqual({active: true})
  })
})
