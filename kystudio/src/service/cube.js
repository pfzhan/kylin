import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getCubesList: (params) => {
    return Vue.resource(apiUrl + 'cubes').get(params)
  },
  getCubeDesc: (cubeName) => {
    return Vue.resource(apiUrl + 'cube_desc/' + cubeName).get()
  },
  deleteCube: (cubeName) => {
    return Vue.resource(apiUrl + 'cubes/' + cubeName).delete({})
  },
  rebuildCube: (cube) => {
    return Vue.resource(apiUrl + 'cubes/' + cube.cubeName + '/rebuild').update(cube.timeZone)
  },
  rebuildStreamingCube: (cubeName) => {
    return Vue.resource(apiUrl + 'cubes/' + cubeName + '/build2').update({
      sourceOffsetStart: 0,
      sourceOffsetEnd: '9223372036854775807',
      buildType: 'BUILD'
    })
  },
  enableCube: (cubeName) => {
    return Vue.resource(apiUrl + 'cubes/' + cubeName + '/enable').update({})
  },
  disableCube: (cubeName) => {
    return Vue.resource(apiUrl + 'cubes/' + cubeName + '/disable').update({})
  },
  purgeCube: (cubeName) => {
    return Vue.resource(apiUrl + 'cubes/' + cubeName + '/purge').update({})
  },
  cloneCube: (cube) => {
    return Vue.resource(apiUrl + 'cubes/' + cube.originalName + '/clone').update({cubeName: cube.cubeName, project: cube.project})
  },
  backupCube: (cubeName) => {
    return Vue.resource(apiUrl + 'metastore/backup').save({cube: cubeName})
  },
  updateCube: (cube) => {
    return Vue.resource(apiUrl + 'cubes').update(cube)
  },
  draftCube: (cube) => {
    return Vue.resource(apiUrl + 'cubes/draft').update(cube)
  },
  getCubeSql: (cubeName) => {
    return Vue.resource(apiUrl + 'cubes/' + cubeName + '/sql').get()
  },
  getColumnarInfo: (cubeName) => {
    return Vue.resource(apiUrl + 'cubes/' + cubeName + '/columnar').get()
  },
  getHbaseInfo: (cubeName) => {
    return Vue.resource(apiUrl + 'cubes/' + cubeName + '/hbase').get()
  },
  saveCube: (cube) => {
    return Vue.resource(apiUrl + 'cubes').save(cube)
  },
  checkCubeNameAvailability: (cubeName) => {
    return Vue.resource(apiUrl + 'cube_desc/' + cubeName).get()
  },
  calCuboid: (cubeDesc) => {
    return Vue.resource(apiUrl + 'cubes/aggregationgroups/cuboid').save(cubeDesc)
  },
  getEncoding: () => {
    return Vue.resource(apiUrl + 'encodings/valid_encodings').get()
  },
  getEncodingVersion: () => {
    return Vue.resource(apiUrl + 'cubes/validEncodings').get()
  },
  getRawTable: (cubeName) => {
    return Vue.resource(apiUrl + 'rawtables/' + cubeName).get()
  },
  updateRawTable: (rawTable) => {
    return Vue.resource(apiUrl + 'rawtables').update(rawTable)
  },
  deleteRawTable: (rawTable) => {
    return Vue.resource(apiUrl + 'rawtables/' + rawTable).delete()
  },
  saveRawTable: (rawTable) => {
    return Vue.resource(apiUrl + 'rawtables').save(rawTable)
  },
  saveSampleSql: (cubeDesc) => {
    return Vue.resource(apiUrl + 'smart/' + cubeDesc.modelName + '/' + cubeDesc.cubeName + '/collect_sql').save(cubeDesc.sqls)
  },
  getSampleSql: (cubeName) => {
    return Vue.resource(apiUrl + 'smart/' + cubeName + '/get_sql').get()
  },
  getCubeSuggestions: (cubeDesc) => {
    return Vue.resource(apiUrl + 'smart/suggestions').save(cubeDesc)
  },
  getScheduler: (cubeName) => {
    return Vue.resource(apiUrl + 'cubes/scheduler/' + cubeName).get()
  },
  updateScheduler: (scheduler) => {
    return Vue.resource(apiUrl + 'cubes/' + scheduler.cubeName + '/schedule').update(scheduler.desc)
  },
  deleteScheduler: (cubeName) => {
    return Vue.resource(apiUrl + 'cubes/scheduler/' + cubeName + '/delete').delete()
  },
  saveCubeAccess: (accessData, cubeId) => {
    return Vue.resource(apiUrl + 'access/CubeInstance/' + cubeId).save(accessData)
  },
  editCubeAccess: (accessData, cubeId) => {
    return Vue.resource(apiUrl + 'access/CubeInstance/' + cubeId).update(accessData)
  },
  getCubeAccess: (cubeId) => {
    return Vue.resource(apiUrl + 'access/CubeInstance/' + cubeId).get()
  },
  delCubeAccess: (cubeId, aid) => {
    return Vue.resource(apiUrl + 'access/CubeInstance/' + cubeId).delete({
      accessEntryId: aid
    })
  }

}
