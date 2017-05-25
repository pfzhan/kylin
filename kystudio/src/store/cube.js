import api from './../service/api'
import * as types from './types'
export default {
  state: {
    cubesList: [],
    cubesDescList: []
  },
  mutations: {
    [types.SAVE_CUBES_LIST]: function (state, { list, total }) {
      state.cubesList = list
      state.totalCubes = total
      state.cubesDescList = Object.assign({}, list)
    }
  },
  actions: {
    [types.GET_CUBES_LIST]: function ({ commit }, cube) {
      return api.cube.getCubesList(cube)
    },
    [types.LOAD_CUBE_DESC]: function ({ commit }, cubeName) {
      return api.cube.getCubeDesc(cubeName)
    },
    [types.DELETE_CUBE]: function ({ commit }, cubeName) {
      return api.cube.deleteCube(cubeName)
    },
    [types.REBUILD_CUBE]: function ({ commit }, cube) {
      return api.cube.rebuildCube(cube)
    },
    [types.REBUILD_STREAMING_CUBE]: function ({ commit }, cubeName) {
      return api.cube.rebuildStreamingCube(cubeName)
    },
    [types.ENABLE_CUBE]: function ({ commit }, cubeName) {
      return api.cube.enableCube(cubeName)
    },
    [types.DISABLE_CUBE]: function ({ commit }, cubeName) {
      return api.cube.disableCube(cubeName)
    },
    [types.PURGE_CUBE]: function ({ commit }, cubeName) {
      return api.cube.purgeCube(cubeName)
    },
    [types.CLONE_CUBE]: function ({ commit }, cube) {
      return api.cube.cloneCube(cube)
    },
    [types.BACKUP_CUBE]: function ({ commit }, cubeName) {
      return api.cube.backupCube(cubeName)
    },
    [types.UPDATE_CUBE]: function ({ commit }, cube) {
      return api.cube.updateCube(cube)
    },
    [types.GET_CUBE_SQL]: function ({ commit }, cubeName) {
      return api.cube.getCubeSql(cubeName)
    },
    [types.GET_COLUMNAR_INFO]: function ({ commit }, cubeName) {
      return api.cube.getColumnarInfo(cubeName)
    },
    [types.GET_HBASE_INFO]: function ({ commit }, cubeName) {
      return api.cube.getHbaseInfo(cubeName)
    },
    [types.SAVE_CUBE]: function ({ commit }, cube) {
      return api.cube.saveCube(cube)
    },
    [types.DRAFT_CUBE]: function ({ commit }, cube) {
      return api.cube.draftCube(cube)
    },
    [types.CHECK_CUBE_NAME_AVAILABILITY]: function ({ commit }, cubeName) {
      return api.cube.checkCubeNameAvailability(cubeName)
    },
    [types.CAL_CUBOID]: function ({ commit }, cubeDesc) {
      return api.cube.calCuboid(cubeDesc)
    },
    [types.GET_ENCODING]: function ({ commit }, aggGroup) {
      return api.cube.getEncoding()
    },
    [types.GET_ENCODING_VERSION]: function ({ commit }, aggGroup) {
      return api.cube.getEncodingVersion()
    },
    [types.GET_RAW_TABLE]: function ({ commit }, cubeName) {
      return api.cube.getRawTable(cubeName)
    },
    [types.SAVE_RAW_TABLE]: function ({ commit }, rawTable) {
      return api.cube.saveRawTable(rawTable)
    },
    [types.DELETE_RAW_TABLE]: function ({ commit }, rawTable) {
      return api.cube.deleteRawTable(rawTable)
    },
    [types.UPDATE_RAW_TABLE]: function ({ commit }, rawTable) {
      return api.cube.updateRawTable(rawTable)
    },
    [types.SAVE_SAMPLE_SQL]: function ({ commit }, cubeDesc) {
      return api.cube.saveSampleSql(cubeDesc)
    },
    [types.GET_SAMPLE_SQL]: function ({ commit }, cubeName) {
      return api.cube.getSampleSql(cubeName)
    },
    [types.GET_CUBE_SUGGESTIONS]: function ({ commit }, cubeDesc) {
      return api.cube.getCubeSuggestions(cubeDesc)
    },
    [types.GET_SCHEDULER]: function ({ commit }, cubeName) {
      return api.cube.getScheduler(cubeName)
    },
    [types.UPDATE_SCHEDULER]: function ({ commit }, scheduler) {
      return api.cube.updateScheduler(scheduler)
    },
    [types.DELETE_SCHEDULER]: function ({ commit }, cubeName) {
      return api.cube.deleteScheduler(cubeName)
    }
  },
  getters: {}
}

