import api from './../service/api'
import vue from 'vue'
import * as types from './types'
export default {
  state: {
    cubesList: [],
    cubesDescList: [],
    cubesEditList: [],
    cubeAdd: {}
  },
  mutations: {
    [types.SAVE_CUBES_LIST]: function (state, { list }) {
      state.cubesList = list
      state.cubesDescList = Object.assign({}, list)
      state.cubesEditList = Object.assign({}, list)
    },
    [types.SAVE_CUBE_DESC]: function (state, { desc, index }) {
      vue.set(state.cubesDescList, index, desc[0])
    },
    [types.SAVE_CUBE_EDIT]: function (state, { desc, index }) {
      vue.set(state.cubesEditList, index, desc[0])
    }
  },
  actions: {
    [types.LOAD_CUBES_LIST]: function ({ commit }) {
      api.cube.getCubesList({limit: 15, offset: 0, projectName: localStorage.getItem('selected_project')}).then((response) => {
        commit(types.SAVE_CUBES_LIST, { list: response.data })
      })
    },
    [types.LOAD_CUBE_DESC]: function ({ commit }, cube) {
      api.cube.getCubeDesc(cube.name).then((response) => {
        commit(types.SAVE_CUBE_DESC, { desc: response.data, index: cube.index })
      })
    },
    [types.LOAD_CUBE_EDIT]: function ({ commit }, cube) {
      api.cube.getCubeDesc(cube.name).then((response) => {
        commit(types.SAVE_CUBE_EDIT, { desc: response.data, index: cube.index })
      })
    },
    [types.DELETE_CUBE]: function ({ commit }, cubeName) {
      api.cube.deleteCube(cubeName)
    },
    [types.REBUILD_CUBE]: function ({ commit }, cubeName) {
      api.cube.rebuildCube(cubeName)
    }
  },
  getters: {}
}

