import api from './../service/api'
import * as types from './types'
export default {
  state: {
    defauleConfig: {}
  },
  mutations: {
    [types.SAVE_DEFAULT_CONFIG]: function (state, { list }) {
      state.defauleConfig = list
    }
  },
  actions: {
    [types.LOAD_DEFAULT_CONFIG]: function ({ commit }) {
      api.cube.getDefaults().then((response) => {
        commit(types.SAVE_DEFAULT_CONFIG, { list: response.data })
      })
    }
  },
  getters: {}
}

