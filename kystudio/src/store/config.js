import api from './../service/api'
import * as types from './types'
export default {
  state: {
    defaultConfig: {}
  },
  mutations: {
    [types.SAVE_DEFAULT_CONFIG]: function (state, { list }) {
      state.defaultConfig = list
    }
  },
  actions: {
    [types.LOAD_DEFAULT_CONFIG]: function ({ commit }) {
      api.config.getDefaults().then((response) => {
        commit(types.SAVE_DEFAULT_CONFIG, { list: response.data })
      })
    }
  },
  getters: {}
}

