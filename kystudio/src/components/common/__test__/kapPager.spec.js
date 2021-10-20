import { mount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import kapPager from '../kap_pager.vue'
import * as utils from '../../../util'

const mockCacheLocalStorage = jest.spyOn(utils, 'cacheLocalStorage').mockImplementation()

const wrapper = mount(kapPager, {
  localVue,
  propsData: {
    perPageSize: 10,
    totalSize: 50

  }
})

describe('Component KapPager', () => {
  it('methods', () => {
    wrapper.vm.pageChange(2)
    expect(wrapper.vm.currentPage).toBe(2)
    expect(wrapper.emitted().handleCurrentChange).toEqual([[1, 10]])

    wrapper.vm.sizeChange(20)
    expect(wrapper.vm.pageSize).toBe(20)
    expect(wrapper.vm.currentPage).toBe(0)
    expect(mockCacheLocalStorage).toBeCalledWith('', 20)
  })
})

