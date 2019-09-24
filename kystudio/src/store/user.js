import api from './../service/api'
import * as types from './types'
import { permissions } from '../config'
import { getAvailableOptions } from '../util/specParser'
export default {
  state: {
    usersList: [],
    usersSize: 0,
    usersGroupList: [],
    usersGroupSize: 0,
    currentUser: null,
    currentUserAccess: null,
    userDetail: null,
    isShowAdminTips: true
  },
  mutations: {
    [types.SAVE_USERS_LIST]: function (state, result) {
      state.usersList = result.list
      state.usersSize = result.size
    },
    [types.SAVE_GROUP_USERS_LIST]: function (state, result) {
      state.usersGroupList = result.list
      state.usersGroupSize = result.size
    },
    [types.SAVE_CURRENT_LOGIN_USER]: function (state, result) {
      state.currentUser = result.user
    },
    [types.SAVE_CURRENT_USER_ACCESS]: function (state, result) {
      state.currentUserAccess = result.access
    }
  },
  actions: {
    [types.LOAD_USERS_LIST]: function ({ commit, state }, para) {
      return api.user.getUsersList(para).then((response) => {
        commit(types.SAVE_USERS_LIST, { list: response.data.data.users, size: response.data.data.size })
        return response
      }, (res) => {
        state.usersList = []
        state.usersSize = 0
        return res
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
    },
    [types.ADD_USERS_TO_GROUP]: function ({ commit }, para) {
      return api.user.addUsersToGroup(para)
    },
    [types.ADD_GROUPS_TO_USER]: function ({ commit }, para) {
      return api.user.addGroupsToUser(para)
    },
    [types.ADD_GROUP]: function ({ commit }, para) {
      return api.user.addGroup(para)
    },
    [types.DEL_GROUP]: function ({ commit }, para) {
      return api.user.delGroup(para)
    },
    [types.GET_GROUP_LIST]: function ({ commit }, para) {
      return api.user.getGroupList(para)
    },
    [types.GET_USERS_BY_GROUPNAME]: function ({ commit, state }, para) {
      return api.user.getUsersByGroupName(para).then((response) => {
        commit(types.SAVE_USERS_LIST, { list: response.data.data.groupMembers, size: response.data.data.size })
        return response
      }, (res) => {
        state.usersList = []
        state.usersSize = 0
        return res
      })
    },
    [types.GET_GROUP_USERS_LIST]: function ({ commit }, para) {
      return api.user.getUserGroupList(para).then((response) => {
        commit(types.SAVE_GROUP_USERS_LIST, { list: response.data.data.usersWithGroup, size: response.data.data.size })
      })
    },
    [types.USER_ACCESS]: function ({ commit }, para) {
      return new Promise((resolve, reject) => {
        api.user.userAccess({project: para.project}).then((res) => {
          if (!para.notCache) {
            commit(types.SAVE_CURRENT_USER_ACCESS, {access: res.data.data})
          }
          resolve(res)
        }, () => {
          reject()
        })
      })
    }
  },
  getters: {
    userAuthorities (state) {
      const { authorities = [] } = state.currentUser || {}
      return authorities.map(authority => authority.authority)
    },
    userActions (state, getters, rootState, rootGetters) {
      const groupRole = getters.userAuthorities
      const projectRole = state.currentUserAccess

      return getAvailableOptions('userActions', { groupRole, projectRole })
    },
    groupActions (state, getters, rootState, rootGetters) {
      const groupRole = getters.userAuthorities
      const projectRole = state.currentUserAccess

      return getAvailableOptions('groupActions', { groupRole, projectRole })
    },
    isAdminRole (state) {
      const { currentUser } = state

      return currentUser &&
        currentUser.authorities &&
        currentUser.authorities.some(({authority}) => authority === 'ROLE_ADMIN')
    },
    isProjectAdmin (state) {
      return [
        permissions.ADMINISTRATION.value
      ].includes(state.currentUserAccess)
    },
    isProjectManager (state) {
      return [
        permissions.ADMINISTRATION.value,
        permissions.MANAGEMENT.value
      ].includes(state.currentUserAccess)
    }
  }
}

