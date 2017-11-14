import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getUsersList: (para) => {
    return Vue.resource(apiUrl + 'kap/user/users').get(para)
  },
  updateStatus: (user) => {
    return Vue.resource(apiUrl + 'kap/user/' + user.name).update({disabled: user.disabled, defaultPassword: true})
  },
  saveUser: (user) => {
    return Vue.resource(apiUrl + 'kap/user/' + user.name).save(user.detail)
  },
  editRole: (user) => {
    return Vue.resource(apiUrl + 'kap/user/' + user.name).update(user.detail)
  },
  resetPassword: (user) => {
    return Vue.resource(apiUrl + 'kap/user/password').update(user)
  },
  removeUser: (userName) => {
    return Vue.resource(apiUrl + 'kap/user/' + userName).remove()
  },
  // access
  login: () => {
    return Vue.resource(apiUrl + 'user/authentication').save()
  },
  loginOut: () => {
    return Vue.resource(apiUrl + 'j_spring_security_logout').get()
  },
  authentication: () => {
    return Vue.resource(apiUrl + 'user/authentication').get()
  },
  userAccess: (para) => {
    return Vue.resource(apiUrl + 'access/user/permission' + '/' + para.project).get()
  },
  // user goup
  addGroupsToUser: (para) => {
    return Vue.resource(apiUrl + 'kap/user/' + para.username).update(para)
  },
  addUsersToGroup: (para) => {
    return Vue.resource(apiUrl + 'user_group/users/' + para.groupName).save(para.data)
  },
  getUserGroupList: (para) => {
    return Vue.resource(apiUrl + 'user_group/usersWithGroup').get(para)
  },
  getGroupList: (para) => {
    return Vue.resource(apiUrl + 'user_group/groups?project=' + para.project).get()
  },
  addGroup: (para) => {
    return Vue.resource(apiUrl + 'user_group/' + para.groupName).save()
  },
  delGroup: (para) => {
    return Vue.resource(apiUrl + 'user_group/' + para.groupName).remove()
  },
  getUsersByGroupName: (para) => {
    return Vue.resource(apiUrl + 'user_group/groupMembers/' + para.groupName).get(para)
  }
}
