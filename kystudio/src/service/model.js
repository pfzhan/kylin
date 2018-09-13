import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  // 获取加速信息
  getSpeedModelInfo: (projectName) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/threshold?project=' + projectName).get()
  },
  // 执行加速
  applySpeedModelInfo: (projectName, size) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/accept?project=' + projectName + '&accelerateSize=' + size).update()
  },
  ignoreSpeedModelInfo: (projectName) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/ignore/' + projectName).update()
  },
  // purge
  purgeModel: (project, modelName) => {
    return Vue.resource(apiUrl + 'models/segments/' + project + '/' + modelName).delete()
  },
  getModelList: (params) => {
    return Vue.resource(apiUrl + 'models').get(params)
  },
  renameModel: (params) => {
    return Vue.resource(apiUrl + 'models/name').update(params)
  },
  disableModel: (params) => {
    params.status = 'DISABLED'
    return Vue.resource(apiUrl + 'models/status').update(params)
  },
  enableModel: (params) => {
    params.status = 'READY'
    return Vue.resource(apiUrl + 'models/status').update(params)
  },
  measureDimensionSuggestion: (params) => {
    return Vue.resource(apiUrl + 'models/' + params.project + '/table_suggestions').get(params)
  },
  getModelByModelName: (para) => {
    return Vue.resource(apiUrl + 'models').get(para)
  },
  deleteModel: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.project + '/' + para.modelName).delete()
  },
  collectStats: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.project + '/' + para.modelname + '/stats').save(para.data)
  },
  updateModel: (data) => {
    return Vue.resource(apiUrl + 'models/').update(data)
  },
  saveModel: (data) => {
    return Vue.resource(apiUrl + 'models').update(data)
  },
  saveModelDraft: (data) => {
    return Vue.resource(apiUrl + 'models/draft').update(data)
  },
  cloneModel: (para) => {
    return Vue.resource(apiUrl + 'models').save(para)
  },
  diagnose: (project, modelName) => {
    return Vue.resource(apiUrl + 'models/' + project + '/' + modelName + '/diagnose').get()
  },
  diagnoseList: (para) => {
    return Vue.resource(apiUrl + 'models/get_all_stats').get(para)
  },
  checkModelName: (para) => {
    return Vue.resource(apiUrl + 'models/validate/' + para.modelName).get()
  },
  checkUsedCols: (modelName) => {
    return Vue.resource(apiUrl + 'models/' + modelName + '/usedCols').get()
  },
  modelProgress: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.project + '/' + para.modelName + '/progress').get()
  },
  modelCheckable: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.project + '/' + para.modelName + '/checkable').get()
  },
  getModelAccess: (modelId) => {
    return Vue.resource(apiUrl + 'access/DataModelDesc/' + modelId).get()
  },
  getModelEndAccess: (modelId) => {
    return Vue.resource(apiUrl + 'access/all/DataModelDesc/' + modelId).get()
  },
  validModelPartitionColumnFormat: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.project + '/' + para.table + '/' + para.column + '/validate').get({format: para.format})
  },
  getColumnSampleData: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.project + '/' + para.table + '/' + para.column).get()
  },
  checkComputedExpression: (para) => {
    return Vue.resource(apiUrl + 'models/validness').save(para)
  },
  getComputedColumns: (projectName) => {
    return Vue.resource(apiUrl + 'models/computed_column_usage/' + projectName).get()
  },
  sqlValidate: (para) => {
    return Vue.resource(apiUrl + 'sql_validate/model').save(para)
  },
  autoModel: (para) => {
    return Vue.resource(apiUrl + 'smart/model').save(para)
  },
  validAutoModelSql: (para) => {
    return Vue.resource(apiUrl + 'smart/validate_sqls').save(para)
  },
  getAutoModelSql: (para) => {
    return Vue.resource(apiUrl + 'smart/' + para.modelName + '/model_sqls').get()
  },
  fetchSegments: (model, project, startTime, endTime) => {
    return Vue.resource(`${apiUrl}models/segments`).get({model, project, startTime, endTime})
  },
  fetchAggregates: (model, project) => {
    return Vue.resource(`${apiUrl}models/agg_indexs`).get({model, project})
  },
  fetchCuboid: (model, project, id) => {
    return Vue.resource(`${apiUrl}models/cuboids`).get({model, project, id})
  },
  fetchCuboids: (model, project) => {
    return Vue.resource(`${apiUrl}models/relations`).get({model, project})
  }
}
