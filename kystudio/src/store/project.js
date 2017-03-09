import api from './../service/api'
import * as types from './types'
export default {
  state: {
    projectList: []
  },
  mutations: {
    [types.SAVE_PROJECT_LIST]: function (state, { list }) {
      state.projectList = list
    }
  },
  actions: {
    [types.LOAD_PROJECT_LIST]: function ({ commit }) {
      api.project.getProjectList().then((response) => {
        commit(types.SAVE_PROJECT_LIST, { list: response.data })
      }, () => {
      })
    },
    [types.DELETE_PROJECT]: function ({ commit }, projectName) {
      api.project.deleteProject(projectName)
    },
    [types.UPDATE_PROJECT]: function ({ commit }, project) {
      api.project.updateProject(project)
    },
    [types.SAVE_PROJECT]: function ({ commit }, project) {
      api.project.saveProject(project)
    }
  },
  getters: {}
}
