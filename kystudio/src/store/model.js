import api from './../service/api'
import * as types from './types'
export default {
  state: {
    modelsList: []
  },
  mutations: {
    [types.SAVE_MODEL_LIST]: function (state, { list }) {
      state.modelsList = list
    }
  },
  actions: {
    [types.LOAD_MODEL_LIST]: function ({ commit }) {
      api.model.getModelList({projectName: localStorage.getItem('selected_project')}).then((response) => {
        commit(types.SAVE_MODEL_LIST, { list: response.data })
      })
    }
  },
  getters: {}
}

