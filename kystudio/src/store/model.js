import api from './../service/api'
import * as types from './types'
export default {
  state: {
    modelsList: [],
    modelsTotal: 0
  },
  mutations: {
    [types.SAVE_MODEL_LIST]: function (state, result) {
      console.log(result)
      state.modelsList = result.list
      state.modelsTotal = result.total
    }
  },
  actions: {
    [types.LOAD_MODEL_LIST]: function ({ commit }, para) {
      api.model.getModelList(para).then((response) => {
        console.log(response)
        commit(types.SAVE_MODEL_LIST, { list: response.data.data.list, total: response.data.data.total })
      })
    }
  },
  getters: {}
}

