import { mount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import * as business from '../../../util/business'
import Group from '../Group/index.vue'
import GroupEditModal from '../../common/GroupEditModal/index.vue'
import kapPager from '../../common/kap_pager.vue'
import Vuex from 'vuex'

const mockLoadGroupUsersList = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    reject(new Error('error'))
  })
})
const mockDelGroup = jest.fn().mockImplementation()
const mockLoadUsersList = jest.fn().mockImplementation(() => {
  return new Promise((resolve) => resolve({ status: 201, data: '', msg: '' }))
})
const mockGroupEditModal = jest.fn().mockResolvedValue(true)

const mockHandleError = jest.spyOn(business, 'handleError').mockImplementation(() => {
  return new Promise((resolve, reject) => reject('error'))
})
const kapConfirmMockHandle = jest.spyOn(business, 'kapConfirm').mockRejectedValue(false)

const GroupEditModalStore = {
  namespaced: true,
  state: {
    totalUsers: []
  }
}

const store = new Vuex.Store({
  getters: {
    groupActions () {
      return ['viewGroup', 'addGroup']
    },
    currentSelectedProject () {
      return 'test'
    }
  },
  state: {
    user: {
      usersGroupSize: 10,
      usersList: ['admin', 'test'],
      usersGroupList: []
    }
  },
  actions: {
    'GET_GROUP_USERS_LIST': mockLoadGroupUsersList,
    'DEL_GROUP': mockDelGroup,
    'LOAD_USERS_LIST': mockLoadUsersList
  },
  modules: {
    GroupEditModal: GroupEditModalStore
  }
})

// const editModal = shallowMount(GroupEditModal, { localVue, store })

const wrapper = mount(Group, {
  store,
  localVue,
  mocks: {
    handleError: mockHandleError,
    kapConfirm: kapConfirmMockHandle
  },
  components: {
    GroupEditModal,
    kapPager
  }
})

describe('Component Group', () => {
  it('init', () => {
    expect(mockLoadGroupUsersList).toBeCalled()
    expect(mockLoadUsersList).toBeCalled()
    expect(mockHandleError).toBeCalled()
    expect(wrapper.vm.$data.pagination.page_size).toBe(20)

    // console.log(wrapper.html())
    const options = {
      callGroupEditModal: mockGroupEditModal,
      loadGroupUsers: mockLoadGroupUsersList
    }
    Group.options.methods.editGroup.call(options)
    expect(mockGroupEditModal).toBeCalled()
    expect(mockLoadGroupUsersList).toBeCalled()
  })
  it('computed', async () => {
    expect(wrapper.vm.emptyText).toBe('No data')
    await wrapper.setData({filterName: 'test'})
    expect(wrapper.vm.emptyText).toBe('No Results')
  })
  it('methods event', async () => {
    await wrapper.vm.inputFilter('content')
    expect(wrapper.vm.$data.filterName).toBe('content')
    expect(wrapper.vm.$data.pagination.page_size).toBe(20)

    wrapper.vm.handleCurrentChange(2, 50)
    expect(wrapper.vm.$data.pagination.page_offset).toBe(2)
    expect(wrapper.vm.$data.pagination.page_size).toBe(50)

    await wrapper.vm.dropGroup()
    // expect(kapConfirmMockHandle).toBeCalledWith('Are you sure you want to delete the group ?', null, 'Delete Group')
    expect(mockHandleError).toBeCalled()
  })
})
