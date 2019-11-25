import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  // 获取加速信息
  getSpeedModelInfo: (projectName) => {
    const vm = window.kapVm
    return vm.$http.get(apiUrl + 'query/favorite_queries/threshold?project=' + projectName, {headers: {'X-Progress-Invisiable': 'true'}})
  },
  // 执行加速
  applySpeedModelInfo: (projectName, size) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/accept?project=' + projectName + '&accelerateSize=' + size).update()
  },
  ignoreSpeedModelInfo: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/ignore?project=' + para.project + '&ignoreSize=' + para.ignoreSize).update()
  },
  // purge
  purgeModel: (project, modelId) => {
    return Vue.resource(apiUrl + 'models/segments/' + project + '/' + modelId).delete()
  },
  getModelList: (params) => {
    return Vue.resource(apiUrl + 'models').get(params)
  },
  renameModel: (params) => {
    return Vue.resource(apiUrl + 'models/name').update(params)
  },
  disableModel: (params) => {
    params.status = 'OFFLINE'
    return Vue.resource(apiUrl + 'models/status').update(params)
  },
  enableModel: (params) => {
    params.status = 'ONLINE'
    return Vue.resource(apiUrl + 'models/status').update(params)
  },
  measureDimensionSuggestion: (params) => {
    return Vue.resource(apiUrl + 'models/' + params.project + '/table_suggestions').get(params)
  },
  getModelByModelName: (para) => {
    return Vue.resource(apiUrl + 'models').get(para)
  },
  deleteModel: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.project + '/' + para.modelId).delete()
  },
  collectStats: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.project + '/' + para.modelname + '/stats').save(para.data)
  },
  updateModel: (data) => {
    return Vue.resource(apiUrl + 'models/semantic').update(data)
  },
  saveModel: (data) => {
    return Vue.resource(apiUrl + 'models').save(data)
  },
  saveModelDraft: (data) => {
    return Vue.resource(apiUrl + 'models/draft').update(data)
  },
  cloneModel: (para) => {
    return Vue.resource(apiUrl + 'models/clone').save(para)
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
    return Vue.resource(apiUrl + 'models/computed_columns/check').save(para)
  },
  getComputedColumns: (para) => {
    return Vue.resource(apiUrl + 'models/computed_columns/usage').get(para)
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
  fetchSegments: (model, project, start, end, sortBy, reverse, pageOffset, pageSize) => {
    return Vue.resource(`${apiUrl}models/segments`).get({model, project, start, end, sortBy, reverse, pageOffset, pageSize})
  },
  fetchAggregates: (para) => {
    return Vue.resource(`${apiUrl}models/agg_indices`).get(para)
  },
  fetchCuboid: (model, project, id) => {
    return Vue.resource(`${apiUrl}models/cuboids`).get({model, project, id})
  },
  fetchCuboids: (model, project) => {
    return Vue.resource(`${apiUrl}models/relations`).get({model, project})
  },
  getTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index').get(para)
  },
  editTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index').update(para)
  },
  delTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index/' + para.project + '/' + para.model + '/' + para.tableIndexId).delete()
  },
  addTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index').save(para)
  },
  refreshTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index').save(para)
  },
  refreshSegments: (modelId, project, ids) => {
    return Vue.resource(apiUrl + 'models/segments').update({ modelId, project, ids })
  },
  deleteSegments: (model, project, ids) => {
    return Vue.resource(`${apiUrl}models/segments/${project}/${model}`).delete({ ids })
  },
  // 弃用
  modelDataCheck: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.modelId + '/data_check').update(para.data)
  },
  buildModel: (para) => {
    return Vue.resource(apiUrl + 'models/segments').save(para)
  },
  setPartition: (para) => {
    return Vue.resource(apiUrl + 'models/partition_desc').save(para)
  },
  fetchAggregateGroups: (project, model) => {
    return Vue.resource(apiUrl + 'index_plans/rule').get({ project, model })
  },
  updateAggregateGroups: (project, modelId, dimensions, aggregationGroups, isCatchUp) => {
    return Vue.resource(apiUrl + 'index_plans/rule').update({ project, modelId, dimensions, aggregation_groups: aggregationGroups, load_data: isCatchUp })
  },
  getCalcCuboids: (project, modelId, dimensions, aggregationGroups) => {
    return Vue.resource(apiUrl + 'index_plans/agg_index_count').update({ project, modelId, dimensions, aggregation_groups: aggregationGroups })
  },
  fetchRelatedModelStatus: (project, uuids) => {
    const body = { project, uuids }
    const headers = { 'X-Progress-Invisiable': 'true' }
    return window.kapVm.$http.post(apiUrl + 'models/job_error_status', body, { headers })
  },
  getModelDataNewestRange: (para) => {
    return Vue.resource(apiUrl + 'models/data_range/latest_data').get(para)
  },
  loadModelConfigList: (para) => {
    return Vue.resource(apiUrl + 'models/config').get(para)
  },
  updateModelConfig: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/config').update(para)
  },
  getModelJSON: (para) => {
    return Vue.resource(apiUrl + 'models/json').get(para)
  },
  getModelSql: (para) => {
    return Vue.resource(apiUrl + 'models/sql').get(para)
  },
  buildIndex: (para) => {
    return Vue.resource(apiUrl + 'models/indices').save(para)
  },
  getModelRecommendations: (para) => {
    return Vue.resource(apiUrl + 'models/recommendations').get(para)
  },
  adoptModelRecommendations: (para) => { // 提交优化建议的内容
    return Vue.resource(apiUrl + 'models/recommendations').update(para)
  },
  clearModelRecommendations: (para) => { // 删除优化建议的内容
    return Vue.resource(apiUrl + 'models/recommendations').delete(para)
  },
  getAggIndexContentList: (para) => {
    return Vue.resource(apiUrl + 'models/recommendations/agg_index').get(para)
  },
  getTableIndexContentList: (para) => {
    return Vue.resource(apiUrl + 'models/recommendations/table_index').get(para)
  },
  suggestModel: (para) => {
    return Vue.resource(apiUrl + 'models/suggest_model').save(para)
  },
  saveSuggestModels: (para) => {
    return Vue.resource(apiUrl + `models/${para.project}/batch_save_models`).save(para.models)
  },
  validateModelName: (para) => {
    return Vue.resource(apiUrl + 'models/validate_model').save(para)
  },
  addAggIndexAdvanced: (para) => {
    return Vue.resource(apiUrl + 'models/agg_indices/shard_columns').save(para)
  },
  getAggIndexAdvanced: (para) => {
    return Vue.resource(apiUrl + 'models/agg_indices/shard_columns').get(para)
  },
  loadAllIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/index{?sources}').get(para)
  },
  deleteIndex: (para) => {
    return Vue.resource(apiUrl + `index_plans/index/${para.project}/${para.model}/${para.id}`).delete()
  },
  fetchIndexGraph: (para) => {
    return Vue.resource(apiUrl + 'index_plans/index_graph').get(para)
  }
}
