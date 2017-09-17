import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getCubesList: (params) => {
    return Vue.resource(apiUrl + 'cubes').get(params)
  },
  getCubesSegmentsList: (cubesName) => {
    return Vue.resource(apiUrl + 'cubes/' + cubesName + '/columnar').get()
  },
  getCubeDesc: (para) => {
    return Vue.resource(apiUrl + 'cube_desc/' + para.project + '/' + para.cubeName).get()
  },
  deleteCube: (para) => {
    return Vue.resource(apiUrl + 'cubes/' + para.project + '/' + para.cubeName).delete({})
  },
  rebuildCube: (cube) => {
    return Vue.resource(apiUrl + 'cubes/' + cube.cubeName + '/rebuild').update(cube.timeZone)
  },
  rebuildStreamingCube: (cubeName) => {
    return Vue.resource(apiUrl + 'cubes/' + cubeName + '/rebuild_streaming').update({
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
    return Vue.resource(apiUrl + 'metastore/backup?cube=' + cubeName).save()
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
  checkCubeNameAvailability: (para) => {
    return Vue.resource(apiUrl + 'cubes?cubeName=' + para.cubeName + '&projectName=' + para.project).get()
  },
  calCuboid: (cubeDesc) => {
    // var resultUrl = cubeDesc.aggIndex === -1 ? 'cuboid' : 'aggregationgroups/' + cubeDesc.aggIndex + '/cuboid'
    return Vue.resource(apiUrl + 'cubes/cuboid').save(cubeDesc.cubeDescData)
  },
  // calAllCuboid: (cubeDesc) => {
  //   return Vue.resource(apiUrl + 'cubes/aggregationgroups/cuboid').save(cubeDesc.cubeDescData)
  // },
  getEncoding: () => {
    return Vue.resource(apiUrl + 'encodings/valid_encodings').get()
  },
  getEncodingVersion: () => {
    return Vue.resource(apiUrl + 'cubes/validEncodings').get()
  },
  getRawTable: (para) => {
    return Vue.resource(apiUrl + 'raw_desc/' + para.project + '/' + para.cubeName).get()
  },
  updateRawTable: (rawTable) => {
    return Vue.resource(apiUrl + 'raw_desc').update(rawTable)
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
  checkSql: (cubeDesc) => {
    return Vue.resource(apiUrl + 'smart/' + cubeDesc.modelName + '/' + cubeDesc.cubeName + '/check_sql').save(cubeDesc.sqls)
  },
  getCubeSuggestions: (cubeDesc) => {
    return Vue.resource(apiUrl + 'smart/aggregation_groups').save(cubeDesc)
  },
  getCubeSuggestDimensions: (cubeDesc) => {
    return Vue.resource(apiUrl + 'smart/' + cubeDesc.model + '/' + cubeDesc.cube + '/dimension_measure').save()
  },
  getSqlDimensions: (cubeName) => {
    return Vue.resource(apiUrl + 'smart/' + cubeName + '/sql_dimension').get()
  },
  getScheduler: (para) => {
    return Vue.resource(apiUrl + 'cubes/' + para.project + '/' + para.cubeName + '/scheduler_job').get()
  },
  updateScheduler: (scheduler) => {
    return Vue.resource(apiUrl + 'cubes/' + scheduler.cubeName + '/schedule').update(scheduler.desc)
  },
  deleteScheduler: (cubeName) => {
    return Vue.resource(apiUrl + 'cubes/' + cubeName + '/scheduler_job').delete()
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
  getCubeEndAccess: (cubeId) => {
    return Vue.resource(apiUrl + 'access/all/CubeInstance/' + cubeId).get()
  },
  delCubeAccess: (cubeId, aid) => {
    return Vue.resource(apiUrl + 'access/CubeInstance/' + cubeId).delete({
      accessEntryId: aid
    })
  },
  sqlValidate: (para) => {
    return Vue.resource(apiUrl + 'sql_validate/cube').save(para)
  }

}
