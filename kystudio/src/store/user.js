import api from './../service/api'
import * as types from './types'
export default {
  state: {
    usersList: [],
    usersSize: 0,
    currentUser: null,
    userDetail: null
  },
  mutations: {
    [types.SAVE_USERS_LIST]: function (state, result) {
      state.usersList = result.list
      state.usersSize = result.size
    },
    [types.SAVE_CURRENT_LOGIN_USER]: function (state, result) {
      state.currentUser = result.user
    }
  },
  actions: {
    [types.LOAD_USERS_LIST]: function ({ commit }, para) {
      api.user.getUsersList(para).then((response) => {
        commit(types.SAVE_USERS_LIST, { list: response.data.data.users, size: response.data.data.size })
      })
    },
    [types.UPDATE_STATUS]: function ({ commit }, user) {
      return api.user.updateStatus(user)
    },
    [types.SAVE_USER]: function ({ commit }, user) {
      return api.user.saveUser(user)
    },
    [types.EDIT_ROLE]: function ({ commit }, user) {
      return api.user.editRole(user)
    },
    [types.RESET_PASSWORD]: function ({ commit }, user) {
      return api.user.resetPassword(user)
    },
    [types.REMOVE_USER]: function ({ commit }, userName) {
      return api.user.removeUser(userName)
    },
    [types.LOGIN]: function ({ commit }, user) {
      return api.user.login()
    },
    [types.LOGIN_OUT]: function ({ commit }) {
      return api.user.loginOut()
    },
    [types.USER_AUTHENTICATION]: function ({ commit }) {
      return api.user.authentication()
    }
  },
  getters: {}
}

