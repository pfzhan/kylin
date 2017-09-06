import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getCubesList: (params) => {
    return Vue.resource(apiUrl + 'vubes').get(params)
  },
  getCubesSegmentsList: (cubesName) => {
    return Vue.resource(apiUrl + 'vubes/' + cubesName + '/columnar').get()
  },
  getCubeDesc: (para) => {
    return Vue.resource(apiUrl + 'vubes/' + para.project + '/' + para.cubeName + '/cube_desc').get(para.version)
  },
  deleteCube: (para) => {
    return Vue.resource(apiUrl + 'vubes/' + para.project + '/' + para.cubeName).delete({})
  },
  rebuildCube: (cube) => {
    return Vue.resource(apiUrl + 'vubes/' + cube.cubeName + '/rebuild').update(cube.para)
  },
  rebuildStreamingCube: (cubeName) => {
    return Vue.resource(apiUrl + 'vubes/' + cubeName + '/rebuild_streaming').update({
      sourceOffsetStart: 0,
      sourceOffsetEnd: '9223372036854775807',
      buildType: 'BUILD'
    })
  },
  getSegEndTime: (cubeName) => {
    return Vue.resource(apiUrl + 'vubes/' + cubeName + '/segments_end').get()
  },
  enableCube: (cubeName) => {
    return Vue.resource(apiUrl + 'vubes/' + cubeName + '/enable').update({})
  },
  disableCube: (cubeName) => {
    return Vue.resource(apiUrl + 'vubes/' + cubeName + '/disable').update({})
  },
  purgeCube: (cubeName) => {
    return Vue.resource(apiUrl + 'vubes/' + cubeName + '/purge').update({})
  },
  cloneCube: (cube) => {
    return Vue.resource(apiUrl + 'vubes/' + cube.originalName + '/clone').update({cubeName: cube.cubeName, project: cube.project})
  },
  backupCube: (cubeName) => {
    return Vue.resource(apiUrl + 'metastore/backup?cube=' + cubeName).save()
  },
  updateCube: (cube) => {
    return Vue.resource(apiUrl + 'vubes').update(cube)
  },
  draftCube: (cube) => {
    return Vue.resource(apiUrl + 'cubes/draft').update(cube)
  },
  manageCube: (manage) => {
    return Vue.resource(apiUrl + 'vubes/' + manage.name + '/manage/' + manage.type).update(manage.data)
  },
  getCubeSql: (cubeName) => {
    return Vue.resource(apiUrl + 'vubes/' + cubeName + '/sql').get()
  },
  checkSql: (cubeDesc) => {
    return Vue.resource(apiUrl + 'smart/' + cubeDesc.modelName + '/' + cubeDesc.cubeName + '/check_sql').save(cubeDesc.sqls)
  },
  getColumnarInfo: (cubeName) => {
    return Vue.resource(apiUrl + 'vubes/' + cubeName + '/columnar').get()
  },
  getHbaseInfo: (cubeName) => {
    return Vue.resource(apiUrl + 'vubes/' + cubeName + '/hbase').get()
  },
  getCubeSegments: (para) => {
    return Vue.resource(apiUrl + 'vubes/' + para.name + '/segments').get(para.version)
  },
  getCubeVersions: (cubeName) => {
    return Vue.resource(apiUrl + 'vubes/' + cubeName + '/versions').get()
  },
  saveCube: (cube) => {
    return Vue.resource(apiUrl + 'vubes').save(cube)
  },
  checkCubeNameAvailability: (para) => {
    return Vue.resource(apiUrl + 'vubes?vubeName=' + para.cubeName + '&projectName=' + para.project).get()
  },
  calCuboid: (cubeDesc) => {
    return Vue.resource(apiUrl + 'cubes/cuboid').save(cubeDesc.cubeDescData)
  },
  getEncoding: () => {
    return Vue.resource(apiUrl + 'encodings/valid_encodings').get()
  },
  getEncodingVersion: () => {
    return Vue.resource(apiUrl + 'vubes/validEncodings').get()
  },
  getRawTable: (para) => {
    return Vue.resource(apiUrl + 'raw_desc/' + para.project + '/' + para.cubeName).get()
  },
  getRawTableDesc: (para) => {
    return Vue.resource(apiUrl + 'vubes/' + para.project + '/' + para.cubeName + '/raw_desc').get(para.version)
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
    return Vue.resource(apiUrl + 'vubes/' + cubeName + '/sample_sqls').get()
  },
  getCubeSuggestions: (cubeDesc) => {
    return Vue.resource(apiUrl + 'smart/aggregation_groups').save(cubeDesc)
  },
  getCubeSuggestDimensions: (cubeDesc) => {
    return Vue.resource(apiUrl + 'smart/' + cubeDesc.model + '/' + cubeDesc.cube + '/dimension_measure').save()
  },
  getScheduler: (para) => {
    return Vue.resource(apiUrl + 'vubes/' + para.project + '/' + para.cubeName + '/scheduler_job').get()
  },
  updateScheduler: (scheduler) => {
    return Vue.resource(apiUrl + 'vubes/' + scheduler.cubeName + '/schedule').update(scheduler.desc)
  },
  deleteScheduler: (cubeName) => {
    return Vue.resource(apiUrl + 'vubes/' + cubeName + '/scheduler_job').delete()
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
  }
}
