import api from './../service/api'
import * as types from './types'
export default {
  state: {
    projectList: []
  },
  mutations: {
    [types.SAVE_PROJECT_LIST]: function (state, { list }) {
      console.log(1)
      state.projectList = list
    }
  },
  actions: {
    [types.LOAD_PROJECT_LIST]: function ({ commit }) {
      api.project.getProjectList().then((response) => {
        commit(types.SAVE_PROJECT_LIST, { list: response.data })
      }, () => {
      })
    }
  },
  getters: {}
}
