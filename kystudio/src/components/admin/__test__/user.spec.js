import { shallow, mount } from 'vue-test-utils'
import { localVue } from '../../../../test/common/spec_common'
import * as business from '../../../util/business'
import User from '../User/index.vue'
import Vuex from 'vuex'

const mockHandleError = jest.spyOn(business, 'handleError').mockImplementation(() => {
  return new Promise((resolve, reject) => reject('error'))
})
const kapConfirmMockHandle = jest.spyOn(business, 'kapConfirm').mockResolvedValue(true)
const mockMessage = jest.fn().mockResolvedValue()

const mockRemoveUser = jest.fn().mockImplementation((res) => {
  return new Promise((resolve) => {
    resolve(res)
  })
})
const mockUserList = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    reject()
  })
})
const mockUserByGroupName = jest.fn().mockImplementation(() => {
  return new Promise((resolve) => {
    resolve({
      data: {
        data: {
          value: [{
            username: 'ADMIN',
            disabled: false,
            authorities: [{authority: 'ROLE_ADMIN'}, {authority: 'ROLE_ANALYST'}],
            default_password: false
          }],
          total_size: 50
        }
      },
      status: 201
    })
  })
})
const mockUpdateStatus = jest.fn().mockResolvedValue(true)

const store = new Vuex.Store({
  state: {
    user: {
      currentUser: {},
      usersGroupList: [{group_name: 'ROLE_ADMIN'}]
    }
  },
  getters: {
    userActions () {
      return ['addUser']
    },
    currentSelectedProject () {
      return 'test'
    },
    isTestingSecurityProfile () {
      return false
    }
  },
  actions: {
    'REMOVE_USER': mockRemoveUser,
    'LOAD_USERS_LIST': mockUserList,
    'GET_USERS_BY_GROUPNAME': mockUserByGroupName,
    'UPDATE_STATUS': mockUpdateStatus
  }
})

let factory = (mock) => {
  return mount(User, {
    localVue,
    store,
    mocks: {
      $route: {
        params: {
          groupName: 'ROLE_ADMIN'
        }
      },
      handleError: mockHandleError,
      kapConfirm: kapConfirmMockHandle,
      $message: mockMessage,
      ...mock
    }
  })
}

const wrapper = (() => factory())()

describe('Component User', () => {
  it('init', () => {
    expect(mockUserByGroupName).toBeCalled()
    expect(wrapper.vm.$data.userData).toBeInstanceOf(Array)
    expect(wrapper.vm.$data.totalSize).toBe(50)
  })
  it('computed', async () => {
    expect(wrapper.vm.currentGroup).toEqual({"group_name": "ROLE_ADMIN"})
    expect(wrapper.vm.isActionShow).toBe(1)
    expect(wrapper.vm.isMoreActionShow).toBe(1)
    expect(wrapper.vm.usersList).toBeInstanceOf(Array)
    expect(wrapper.vm.emptyText).toBe('No data')

    wrapper.setData({filterName: 'user'})
    await wrapper.update()
    expect(wrapper.vm.emptyText).toBe('No Results')
  })
  it('mothods', async () => {
    wrapper.vm.inputFilter('test')
    expect(wrapper.vm.pagination).toEqual({'page_offset': 0, 'page_size': 20})
    expect(wrapper.vm.filterName).toBe('test')

    wrapper.vm.handleCurrentChange(1, 50)
    expect(wrapper.vm.pagination).toEqual({'page_offset': 1, 'page_size': 50})
    expect(mockUserByGroupName).toBeCalled()

    await wrapper.vm.dropUser({username: 'xx'})
    expect(kapConfirmMockHandle).toHaveBeenCalledWith('Are you sure to delete the user xx?', null, 'Delete User')
    expect(mockRemoveUser).toBeCalled()
    expect(mockMessage).toBeCalled()

    const options = {
      removeUser: jest.fn().mockRejectedValue(false)
    }
    await User.options.methods.dropUser.call(options, { username: 'xx' })
    expect(kapConfirmMockHandle).toHaveBeenCalledWith('Are you sure to delete the user xx?', null, 'Delete User')
    expect(mockHandleError).toBeCalled()

    await wrapper.vm.changeStatus({disabled: true})
    expect(kapConfirmMockHandle).toHaveBeenCalledWith('Are you sure to Enable the user ?', {'cancelButtonText': 'Cancel', 'confirmButtonText': 'Enable', 'type': 'warning'})
    expect(mockHandleError).toBeCalled()

    const options1 = {
      ...wrapper,
      updateStatus: jest.fn().mockRejectedValue(false),
      $t: (content) => content
    }
    await User.options.methods.changeStatus.call(options1, {disabled: false})
    expect(kapConfirmMockHandle).toHaveBeenCalledWith('Are you sure to Enable the user ?', {'cancelButtonText': 'Cancel', 'confirmButtonText': 'Enable', 'type': 'warning'})
    expect(mockHandleError).toBeCalled()

    const options3 = {
      callUserEditModal: jest.fn().mockResolvedValue(true),
      loadUsers: jest.fn()
    }
    await User.options.methods.editUser.call(options3, 'new', [])
    expect(options3.callUserEditModal).toHaveBeenCalledWith({'editType': 'new', 'userDetail': []})
    expect(options3.loadUsers).toBeCalled()

    const route = {
      to: {
        name: 'User'
      },
      from: {
        name: 'GroupDetail'
      },
      next: jest.fn().mockImplementation(func => func && func(wrapper.vm))
    }
    User.options.beforeRouteEnter(route.to, route.from, route.next)
    expect(route.next).toBeCalled()
    expect(wrapper.vm.$data.filterName).toBe('')
    User.options.beforeRouteEnter(route.to, {name: '/'}, route.next)
    expect(route.next).toBeCalled()

    wrapper.destroy()
  })
  it('test error', async () => {
    const wrapper1 = await factory({
      $route: {
        params: {
          groupName: ''
        }
      }
    })
    expect(mockUserList).toBeCalled()
    expect(mockHandleError).toBeCalled()
    wrapper1.destroy()
  })
})
