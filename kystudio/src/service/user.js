import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl, baseUrl } from '../config'

Vue.use(VueResource)

export default {
  getUsersList: (para) => {
    return Vue.resource(apiUrl + 'kap/user/users').get(para)
  },
  updateStatus: (user) => {
    return Vue.resource(apiUrl + 'kap/user/' + user.name).update({disabled: user.disabled})
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
    return Vue.resource(baseUrl + 'j_spring_security_logout').get()
  },
  authentication: () => {
    return Vue.resource(apiUrl + 'user/authentication').get()
  }

}
