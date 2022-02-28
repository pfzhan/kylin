import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import GroupEditModal from '../../GroupEditModal/index.vue'
import GroupEditStore from '../../GroupEditModal/store'
import { getSubmitData } from '../../GroupEditModal/handler'
import Vuex from 'vuex'
import * as util from '../../../../util/business'

jest.useFakeTimers()

const mockApis = {
  mockAddGroup: jest.fn().mockImplementation(),
  mockLoadUserList: jest.fn().mockImplementation(() => {
    return Promise.resolve({data: {data: {limit: 1000, offset: 0, total_size: 3000, value: [ {authorities: [{authority: 'ALL_USERS'}], defaultPassword: false, disabled: false, locked: false, username: 'admin'} ]}}})
  }),
  mockAddUsersToGroup: jest.fn().mockResolvedValue('')
}
const mockHandleError = jest.spyOn(util, 'handleError').mockRejectedValue(false)
const mockMessage = jest.fn()
const GroupEditModalStore = {
  ...GroupEditStore
}

const root = {
  state: GroupEditStore.state,
  commit: function (name, params) { GroupEditStore.mutations[name](root.state, params) },
  dispatch: function (name, params) { return GroupEditStore.actions[name]({state: root.state, commit: root.commit, dispatch: root.dispatch}, params) }
}

const store = new Vuex.Store({
  state: {},
  actions: {
    'ADD_GROUP': mockApis.mockAddGroup,
    'LOAD_USERS_LIST': mockApis.mockLoadUserList,
    'ADD_USERS_TO_GROUP': mockApis.mockAddUsersToGroup
  },
  getters: {
    currentSelectedProject () {
      return 'ssb'
    },
    isAdminRole () {
      return true
    }
  },
  modules: {
    GroupEditModal: GroupEditModalStore
  }
})

const wrapper = shallowMount(GroupEditModal, {
  localVue,
  store,
  mocks: {
    $message: mockMessage
  }
})

describe('Component GroupEditModal', () => {
  it('computed', async () => {
    expect(wrapper.vm.modalWidth).toBe('440px')
    expect(wrapper.vm.modalTitle).toBe('createGroup')
    wrapper.vm.$store.state.GroupEditModal.editType = 'assign'
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.modalWidth).toBe('600px')
    expect(wrapper.vm.modalTitle).toBe('assignUser')

    expect(wrapper.vm.totalUserData).toEqual([])

    wrapper.vm.$store.state.GroupEditModal.isShow = true
    wrapper.vm.$store.state.GroupEditModal.form.selected_users = ['test_01']
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.$data.isFormShow).toBeTruthy()
    expect(wrapper.vm.$data.page_offset).toBe(1)
    expect(wrapper.vm.$store.state.GroupEditModal.totalUsers).toEqual([{'key': 'admin', 'label': 'admin'}, {'key': 'test_01', 'label': 'test_01'}])
    expect(wrapper.vm.$data.searchValueLeft).toBe('')
    expect(mockApis.mockLoadUserList).toBeCalled()
    expect(wrapper.vm.$data.totalUsersSize).toBe(3000)
  })
  it('methods', async () => {
    wrapper.setData({page_offset: 0})
    await wrapper.vm.$nextTick()
    wrapper.vm.autoLoadMoreData([{authorities: [{authority: 'ALL_USERS'}], defaultPassword: false, disabled: false, locked: false, username: 'kylin'}], '')
    expect(mockApis.mockLoadUserList.mock.calls[2][1]).toEqual({'name': undefined, 'page_offset': 2, 'page_size': 100})
    wrapper.setData({page_offset: 0})
    await wrapper.vm.$nextTick()
    wrapper.vm.autoLoadMoreData([{authorities: [{authority: 'ALL_USERS'}], defaultPassword: false, disabled: false, locked: false, username: 'kylin'}], 'user')
    expect(mockApis.mockLoadUserList).toBeCalled()

    wrapper.vm.$refs.form = {
      validate: jest.fn().mockResolvedValue('')
    }
    await wrapper.vm.submit()
    expect(wrapper.vm.$refs.form.validate).toBeCalled()
    expect(mockApis.mockAddUsersToGroup.mock.calls[0][1]).toEqual({'group_name': '', 'users': ['test_01']})
    expect(mockMessage).toBeCalledWith({'message': 'Adjusted the assigned users and user groups successfully.', 'type': 'success'})
    expect(wrapper.vm.$data.submitLoading).toBeFalsy()

    wrapper.vm.$refs.form = {
      validate: jest.fn().mockRejectedValue('false')
    }
    await wrapper.vm.submit()
    expect(wrapper.vm.$refs.form.validate).toBeCalled()
    expect(wrapper.vm.$data.submitLoading).toBeFalsy()
    expect(mockHandleError).toBeCalled()

    wrapper.vm.transferInputHandler('selected_users', ['abc 123', 'test'])
    expect(wrapper.vm.$store.state.GroupEditModal.form.selected_users).toEqual(['abc 123', 'test'])
    expect(mockApis.mockLoadUserList).toBeCalled()

    wrapper.vm.inputHandler('selected_users', ['abc 123'])
    expect(wrapper.vm.$store.state.GroupEditModal.form.selected_users).toEqual(['abc 123'])

    wrapper.vm.queryHandler('Unassigned Users', 'test')
    jest.runAllTimers()
    // expect(mockApis.mockLoadUserList.mock.calls[6][1]).toEqual({'name': 'test', 'page_offset': 0, 'page_size': 1000})
    expect(wrapper.vm.$store.state.GroupEditModal.totalUsers.length).toBe(2)

    wrapper.vm.queryHandler('Assigned Users', '')
    expect(wrapper.vm.$data.searchValueRight).toBe('')
    expect(wrapper.vm.$data.totalSizes).toEqual([2998, 0])
  })
})

describe('GroupEditModal handler', () => {
  it('submit data', () => {
    expect(getSubmitData({editType: 'new', form: {group_name: 'ALL_USERS'}})).toEqual({'group_name': 'ALL_USERS'})
  })
})

describe('GroupEditModal Store', () => {
  it('actions', () => {
    GroupEditStore.actions['CALL_MODAL'](root, {editType: 'new', group: {first: 'TEST'}})
    expect(GroupEditStore.state.isShow).toBeTruthy()
    expect(GroupEditStore.state.form).toEqual({group_name: undefined, selected_users: undefined})
  })
})
