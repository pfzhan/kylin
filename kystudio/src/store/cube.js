import api from './../service/api'
import * as types from './types'
export default {
  state: {
    cubesList: []
  },
  mutations: {
    [types.SAVE_CUBES_LIST]: function (state, { list }) {
      state.cubesList = list
    }
  },
  actions: {
    [types.LOAD_CUBES_LIST]: function ({ commit }) {
      api.cube.getCubesList({limit: 15, offset: 0, projectName: localStorage.getItem('selected_project')}).then((response) => {
        commit(types.SAVE_CUBES_LIST, { list: response.data })
      })
    }
  },
  getters: {}
}

