import api from './../service/api'
import * as types from './types'
export default {
  state: {
    cubesList: [],
    cubesDescList: [],
    cubeAccess: {},
    cubeEndAccess: {},
    totalCubes: 0,
    cubeRowTableIsSetting: false,
    cubeSchedulerIsSetting: false,
    cubeRawTableBaseData: {}
  },
  mutations: {
    [types.SAVE_CUBES_LIST]: function (state, { list, total }) {
      state.cubesList = list
      state.totalCubes = total
      state.cubesDescList = Object.assign({}, list)
    },
    [types.CACHE_CUBE_ACCESS]: function (state, { access, id }) {
      state.cubeAccess[id] = access
    },
    [types.CACHE_CUBE_END_ACCESS]: function (state, { access, id }) {
      state.cubeEndAccess[id] = access
    },
    [types.CACHE_RAWTABLE__BASEDATA]: function (state, { project, modelName, data }) {
      state.cubeRawTableBaseData[project + '' + modelName] = data
    }
  },
  actions: {
    [types.GET_CUBES_LIST]: function ({ dispatch, commit }, cube) {
      return api.cube.getCubesList(cube).then((res) => {
        commit(types.SAVE_CUBES_LIST, {list: res.data.data.cubes, total: res.data.data.size})
        if (!(cube && cube.ignoreAccess)) {
          var len = res.data.data.cubes && res.data.data.cubes.length || 0
          for (var i = 0; i < len; i++) {
            if (res.data.data.cubes[i].is_draft) {
              continue
            }
          }
        }
        return res
      }, (res) => {
        commit(types.SAVE_CUBES_LIST, {list: [], total: 0})
      })
    },
    [types.GET_CUBES_SEGMENTS_LIST]: function ({ commit }, cubesName) {
      return api.cube.getCubesSegmentsList(cubesName)
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
    [types.PURGE_CUBE]: function ({ commit }, cube) {
      return api.cube.purgeCube(cube)
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
    [types.GET_CUBE_SEGMENTS]: function ({ commit }, para) {
      return api.cube.getCubeSegments(para)
    },
    [types.GET_MP_VALUES]: function ({ commit }, para) {
      return api.cube.getMPValues(para)
    },
    [types.UPDATE_CUBE_SEGMENTS]: function ({ commit }, para) {
      return api.cube.updateCubeSegments(para)
    },
    [types.SAVE_CUBE]: function ({ commit }, cube) {
      return api.cube.saveCube(cube)
    },
    [types.DRAFT_CUBE]: function ({ commit }, cube) {
      return api.cube.draftCube(cube)
    },
    [types.CHECK_CUBE_NAME_AVAILABILITY]: function ({ commit }, para) {
      return api.cube.checkCubeNameAvailability(para)
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
    [types.CHECK_SQL]: function ({ commit }, cubeDesc) {
      return api.cube.checkSql(cubeDesc)
    },
    [types.GET_CUBE_SUGGESTIONS]: function ({ commit }, cubeDesc) {
      return api.cube.getCubeSuggestions(cubeDesc)
    },
    [types.GET_CUBE_DIMENSIONS]: function ({ commit }, cubeDesc) {
      return api.cube.getCubeSuggestDimensions(cubeDesc)
    },
    [types.GET_SQL_DIMENSIONS]: function ({ commit }, cubeName) {
      return api.cube.getSqlDimensions(cubeName)
    },
    [types.GET_SCHEDULER]: function ({ commit }, cubeName) {
      return api.cube.getScheduler(cubeName)
    },
    [types.UPDATE_SCHEDULER]: function ({ commit }, scheduler) {
      return api.cube.updateScheduler(scheduler)
    },
    [types.DELETE_SCHEDULER]: function ({ commit }, cubeName) {
      return api.cube.deleteScheduler(cubeName)
    },
    [types.SAVE_CUBE_ACCESS]: function ({ commit }, {accessData, id}) {
      return api.cube.saveCubeAccess(accessData, id)
    },
    [types.EDIT_CUBE_ACCESS]: function ({ commit }, {accessData, id}) {
      return api.cube.editCubeAccess(accessData, id)
    },
    [types.GET_CUBE_ACCESS]: function ({ commit }, id) {
      return api.cube.getCubeAccess(id).then((res) => {
        commit(types.CACHE_CUBE_ACCESS, {access: res.data.data, id: id})
        return res
      })
    },
    [types.GET_CUBE_END_ACCESS]: function ({ commit }, id) {
      return api.cube.getCubeEndAccess(id).then((res) => {
        commit(types.CACHE_CUBE_END_ACCESS, {access: res.data.data, id: id})
        return res
      })
    },
    [types.DEL_CUBE_ACCESS]: function ({ commit }, {id, aid}) {
      return api.cube.delCubeAccess(id, aid)
    },
    [types.VERIFY_CUBE_SQL]: function ({ commit }, para) {
      return api.cube.sqlValidate(para)
    }
  },
  getters: {}
}

