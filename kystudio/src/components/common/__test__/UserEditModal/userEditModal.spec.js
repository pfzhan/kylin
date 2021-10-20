import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import UserEditModal from '../../UserEditModal/index.vue'
import UserEditStore from '../../UserEditModal/store'
import Vuex from 'vuex'
import * as util from '../../../../util/business'
import { getSubmitData } from '../../UserEditModal/handler'

jest.useFakeTimers()

const mockApis = {
  mockSaveUser: jest.fn().mockImplementation(() => {
    return Promise.resolve()
  }),
  mockEditRole: jest.fn().mockImplementation(),
  mockResetPassword: jest.fn().mockImplementation(),
  mockGetGroupList: jest.fn().mockImplementation(() => {
    return Promise.resolve({data: {data: ['ALL_USERS', 'ROLE_ADMIN', 'ROLE_ANALYST']}})
  }),
  mockAddGroupsToUser: jest.fn().mockImplementation()
}

const mockHandleError = jest.spyOn(util, 'handleError').mockRejectedValue(false)

const mockMessage = jest.fn().mockImplementation()

const UserEditModalStore = {
  ...UserEditStore
}

const store = new Vuex.Store({
  state: {
    user: {
      currentUser: {
        username: 'ADMIN',
        authorities: [{authority: 'ALL_USERS'}],
        defaultPassword: false
      }
    }
  },
  getters: {
    isAdminRole () {
      return true
    },
    currentSelectedProject () {
      return 'ssb'
    }
  },
  actions: {
    'SAVE_USER': mockApis.mockSaveUser,
    'EDIT_ROLE': mockApis.mockEditRole,
    'RESET_PASSWORD': mockApis.mockResetPassword,
    'GET_GROUP_LIST': mockApis.mockGetGroupList,
    'ADD_GROUPS_TO_USER': mockApis.mockAddGroupsToUser
  },
  modules: {
    UserEditModal: UserEditModalStore
  }
})

const root = {
  state: UserEditStore.state,
  commit: function (name, params) { UserEditStore.mutations[name](root.state, params) },
  dispatch: function (name, params) { return UserEditStore.actions[name]({state: root.state, commit: root.commit, dispatch: root.dispatch}, params) }
}

const mockEventListener = jest.spyOn(document, 'addEventListener').mockImplementation()
// const mockRemoveEventListener = jest.spyOn(document, 'removeEventListener').mockImplementation()
const mockFormValidate = jest.fn().mockImplementation(() => {
  return new Promise(resolve => resolve(true))
})

const wrapper = shallowMount(UserEditModal, {
  store,
  localVue,
  mocks: {
    $message: mockMessage,
    $route: {
      params: {
        groupName: 'ROLE_ADMIN'
      }
    }
  },
  $refs: {
    form: {
      validate: jest.fn()
    }
  }
})

// const mockFormValidate = jest.spyOn($refs.form, 'validate').mockImplementation(() => {
//   return new Promise(resolve => resolve(true))
// })

describe('Components UserEditModal', () => {
  it('init', async () => {
    UserEditStore.actions['CALL_MODAL'](root, {editType: 'new', userDetail: {authorities: [{authority: 'ALL_USERS'}]}, showCloseBtn: true, showCancelBtn: true})
    wrapper.vm.$store.state.UserEditModal.isShow = true
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.$data.isFormShow).toBeTruthy()
    expect(wrapper.vm.$data.editType).toBe()
    expect(mockEventListener.mock.calls[0][0]).toBe('keyup')
  })
  it('computed', async () => {
    expect(wrapper.vm.modalWidth).toBe('440px')
    wrapper.vm.$store.state.UserEditModal.editType = 'group'
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.modalWidth).toBe('600px')
    expect(wrapper.vm.modalTitle).toBe('groupMembership')
    wrapper.vm.$store.state.UserEditModal.editType = 'new'
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.modalTitle).toBe('addUser')
  })
  it('methods', async () => {
    expect(wrapper.vm.isFieldShow()).toBeFalsy()
    wrapper.vm.inputHandler('password', '123456789')
    expect(wrapper.vm.$data.pwdRuleList).toEqual({'char': '', 'len': 'ok', 'letter': '', 'num': 'ok'})
    wrapper.vm.inputHandler('password', '123abc')
    expect(wrapper.vm.$data.pwdRuleList).toEqual({'char': '', 'len': 'error', 'letter': 'ok', 'num': 'ok'})
    wrapper.vm.inputHandler('password', '123@abc')
    expect(wrapper.vm.$data.pwdRuleList).toEqual({'char': 'ok', 'len': 'error', 'letter': 'ok', 'num': 'ok'})
    wrapper.vm.inputHandler('password', '123@abcde')
    expect(wrapper.vm.$data.pwdRuleList).toEqual({'char': 'ok', 'len': 'ok', 'letter': 'ok', 'num': 'ok'})
    wrapper.vm.inputHandler('password', '123@abcde.^')
    expect(wrapper.vm.$data.pwdRuleList).toEqual({'char': 'ok', 'len': 'ok', 'letter': 'ok', 'num': 'ok'})
    wrapper.vm.inputHandler('password', '@abcde.^')
    expect(wrapper.vm.$data.pwdRuleList).toEqual({'char': 'ok', 'len': 'ok', 'letter': 'ok', 'num': 'error'})

    await wrapper.vm.fetchUserGroups()
    expect(mockApis.mockGetGroupList.mock.calls[0][1]).toEqual(undefined)
    expect(wrapper.vm.$store.state.UserEditModal.totalGroups).toEqual([{'disabled': true, 'key': 'ALL_USERS', 'label': 'ALL_USERS'}, {'disabled': false, 'key': 'ROLE_ADMIN', 'label': 'ROLE_ADMIN'}, {'disabled': false, 'key': 'ROLE_ANALYST', 'label': 'ROLE_ANALYST'}])

    wrapper.vm.$store.state.UserEditModal.form.password = '123456'
    await wrapper.vm.$nextTick()
    wrapper.vm.blurHandler('password')
    expect(wrapper.vm.$data.pwdRuleList).toEqual({'char': 'error', 'len': 'error', 'letter': 'error', 'num': 'error'})

    wrapper.vm.$store.state.UserEditModal.form.password = 'test'
    await wrapper.vm.$nextTick()
    wrapper.vm.blurHandler('password')
    expect(wrapper.vm.$data.pwdRuleList).toEqual({'char': 'error', 'len': 'error', 'letter': 'error', 'num': 'error'})
    expect(wrapper.vm.$store.state.UserEditModal.form.authorities).toEqual(['ALL_USERS'])

    wrapper.vm.blurHandler('newPassword')
    expect(wrapper.vm.$data.pwdRuleList).toEqual({'char': 'error', 'len': 'error', 'letter': 'error', 'num': 'error'})

    wrapper.vm.$store.state.UserEditModal.form.newPassword = ''
    await wrapper.vm.$nextTick()
    wrapper.vm.blurHandler('newPassword')
    expect(wrapper.vm.$data.pwdRuleList).toEqual({'char': 'error', 'len': 'error', 'letter': 'error', 'num': 'error'})

    expect(wrapper.vm.validate('username')).toBeTruthy()
    expect(wrapper.vm.validate('password')).toBeTruthy()

    await wrapper.vm.submit()
    expect(mockHandleError).toBeCalled()

    wrapper.vm.$refs.form = {
      validate: mockFormValidate
    }
    await wrapper.vm.$nextTick()
    await wrapper.vm.submit()
    expect(wrapper.vm.$store.state.UserEditModal.editType).toBe('new')
    expect(mockApis.mockSaveUser.mock.calls[0][1]).toEqual({'detail': {'authorities': ['ALL_USERS', 'ROLE_ADMIN'], 'disabled': false, 'password': 'dGVzdA==', 'username': ''}, 'name': ''})
    expect(mockMessage).toBeCalledWith({'message': 'Added the user successfully.', 'type': 'success'})

    wrapper.vm.$store.state.UserEditModal.editType = 'password'
    await wrapper.vm.$nextTick()
    await wrapper.vm.submit()
    expect(mockApis.mockResetPassword.mock.calls[0][1]).toEqual({'new_password': '', 'password': '', 'username': ''})
    expect(mockMessage).toBeCalledWith({'message': 'Added the user successfully.', 'type': 'success'})

    wrapper.vm.handlerKeyEvent({keyCode: 13})
    expect(mockHandleError).toBeCalled()

    wrapper.vm.closeHandler()
    jest.runAllTimers()
    expect(wrapper.vm.$store.state.UserEditModal.isShow).toBeFalsy()

    jest.clearAllTimers()
  })
  it('handle events', () => {
    const form = {
      username: 'admin',
      default_password: false,
      disabled: false,
      authorities: ['ALL_USERS']
    }
    const $route = {
      params: {
        groupName: 'TEST_01'
      }
    }
    expect(getSubmitData({editType: 'edit', form})).toEqual({'authorities': ['ALL_USERS'], 'default_password': false, 'disabled': false, 'username': 'admin', 'uuid': undefined})
    expect(getSubmitData({editType: 'group', form})).toEqual({'authorities': ['ALL_USERS'], 'username': 'admin', 'uuid': undefined})
    expect(getSubmitData({editType: 'new', form, $route})).toEqual({'detail': {'authorities': ['ALL_USERS', 'TEST_01'], 'disabled': false, 'password': undefined, 'username': 'admin'}, 'name': 'admin'})
  })
})

describe('UserEditModal Store', () => {
  it('mutations', async () => {
    UserEditStore.mutations['SET_MODAL_FORM'](root.state, {admin: true})
    expect(UserEditStore.state.form.authorities).toEqual(['ALL_USERS', 'ROLE_ADMIN'])
    UserEditStore.mutations['SET_MODAL_FORM'](root.state, {admin: false})
    expect(UserEditStore.state.form.authorities).toEqual(['ALL_USERS'])
  })
})
