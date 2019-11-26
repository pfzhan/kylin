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
    return Vue.resource(apiUrl + 'query/favorite_queries/accept?project=' + projectName + '&accelerate_size=' + size).update()
  },
  ignoreSpeedModelInfo: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/ignore?project=' + para.project + '&ignore_size=' + para.ignoreSize).update()
  },
  // purge
  purgeModel: (project, modelId) => {
    return Vue.resource(apiUrl + 'models/' + modelId + '/segments?project=' + project + '&purge=' + true).delete()
  },
  getModelList: (params) => {
    return Vue.resource(apiUrl + 'models').get(params)
  },
  renameModel: (params) => {
    return Vue.resource(apiUrl + 'models/' + params.model + '/name').update(params)
  },
  disableModel: (params) => {
    params.status = 'OFFLINE'
    return Vue.resource(apiUrl + 'models/' + params.modelId + '/status').update({model: params.modelId, project: params.project, status: params.status})
  },
  enableModel: (params) => {
    params.status = 'ONLINE'
    return Vue.resource(apiUrl + 'models/' + params.modelId + '/status').update({model: params.modelId, project: params.project, status: params.status})
  },
  measureDimensionSuggestion: (params) => {
    return Vue.resource(apiUrl + 'models/table_suggestions').get(params)
  },
  getModelByModelName: (para) => {
    return Vue.resource(apiUrl + 'models').get(para)
  },
  deleteModel: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.modelId + '?project=' + para.project).delete()
  },
  collectStats: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.modelname + '/stats?project=' + para.project).save(para.data)
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
    return Vue.resource(apiUrl + 'models/' + para.model + '/clone').save(para)
  },
  diagnose: (project, modelName) => {
    return Vue.resource(apiUrl + 'models/' + modelName + '/diagnose?project=' + project).get()
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
    return Vue.resource(apiUrl + 'models/' + para.modelName + '/progress?project=' + para.project).get()
  },
  modelCheckable: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.modelName + '/checkable?project=' + para.project).get()
  },
  getModelAccess: (modelId) => {
    return Vue.resource(apiUrl + 'access/data_model_desc/' + modelId).get()
  },
  getModelEndAccess: (modelId) => {
    return Vue.resource(apiUrl + 'access/all/data_model_desc/' + modelId).get()
  },
  validModelPartitionColumnFormat: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.table + '/' + para.column + '/validate?project=' + para.project).get({format: para.format})
  },
  getColumnSampleData: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.table + '/' + para.column + '?project=' + para.project).get()
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
  fetchSegments: (model, project, start, end, sortBy, reverse, page_offset, pageSize) => {
    return Vue.resource(`${apiUrl}models/${model}/segments`).get({model, project, start, end, sort_by: sortBy, reverse, page_offset, page_size: pageSize})
  },
  fetchAggregates: (para) => {
    return Vue.resource(`${apiUrl}models/${para.model}/agg_indices`).get(para)
  },
  fetchCuboid: (model, project, id) => {
    return Vue.resource(`${apiUrl}models/cuboids`).get({model, project, id})
  },
  fetchCuboids: (model, project) => {
    return Vue.resource(`${apiUrl}models/${model}/relations`).get({model, project})
  },
  getTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index').get(para)
  },
  editTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index').update(para)
  },
  delTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index/' + para.model + '/' + para.tableIndexId + '?project=' + para.project).delete()
  },
  addTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index').save(para)
  },
  refreshTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index').save(para)
  },
  refreshSegments: (modelId, project, ids) => {
    return Vue.resource(apiUrl + 'models/' + modelId + '/segments').update({ project, ids })
  },
  deleteSegments: (model, project, ids) => {
    return Vue.resource(`${apiUrl}models/${model}/segments/?project=${project}&purge=false`).delete({ ids })
  },
  // 弃用
  modelDataCheck: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.modelId + '/data_check').update(para.data)
  },
  buildModel: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model_id + '/segments').save(para)
  },
  setPartition: (para) => {
    return Vue.resource(apiUrl + 'models/partition_desc').save(para)
  },
  fetchAggregateGroups: (project, model) => {
    return Vue.resource(apiUrl + 'index_plans/rule').get({ project, model })
  },
  updateAggregateGroups: (project, modelId, dimensions, aggregationGroups, isCatchUp) => {
    return Vue.resource(apiUrl + 'index_plans/rule').update({ project, model_id: modelId, dimensions, aggregation_groups: aggregationGroups, load_data: isCatchUp })
  },
  getCalcCuboids: (project, modelId, dimensions, aggregationGroups) => {
    return Vue.resource(apiUrl + 'index_plans/agg_index_count').update({ project, model_id: modelId, dimensions, aggregation_groups: aggregationGroups })
  },
  fetchRelatedModelStatus: (project, uuids) => {
    const body = { project, uuids }
    const headers = { 'X-Progress-Invisiable': 'true' }
    return window.kapVm.$http.post(apiUrl + 'models/job_error_status', body, { headers })
  },
  getModelDataNewestRange: (para) => {
    return Vue.resource(apiUrl +  `models/${para.model}/data_range/latest_data`).get({project: para.project})
  },
  loadModelConfigList: (para) => {
    return Vue.resource(apiUrl + 'models/config').get(para)
  },
  updateModelConfig: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/config').update(para)
  },
  getModelJSON: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/json').get(para)
  },
  getModelSql: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/sql').get(para)
  },
  buildIndex: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model_id + '/indices').save(para)
  },
  getModelRecommendations: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/recommendations?project=' + para.project).get()
  },
  adoptModelRecommendations: (para) => { // 提交优化建议的内容
    return Vue.resource(apiUrl + 'models/' + para.model + '/recommendations').update(para)
  },
  clearModelRecommendations: (para) => { // 删除优化建议的内容
    return Vue.resource(apiUrl + 'models/' + para.model + '/recommendations').delete(para)
  },
  getAggIndexContentList: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/recommendations/agg_index').get(para)
  },
  getTableIndexContentList: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/recommendations/table_index').get(para)
  },
  getIndexContentList: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/recommendations/index').get(para)
  },
  suggestModel: (para) => {
    return Vue.resource(apiUrl + 'models/suggest_model').save(para)
  },
  saveSuggestModels: (para) => {
    return Vue.resource(apiUrl + `models/batch_save_models?project=${para.project}`).save(para.models)
  },
  validateModelName: (para) => {
    return Vue.resource(apiUrl + 'models/validate_model').save(para)
  },
  addAggIndexAdvanced: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model_id + '/agg_indices/shard_columns').save(para)
  },
  getAggIndexAdvanced: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/agg_indices/shard_columns').get(para)
  },
  loadAllIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/index{?sources}').get(para)
  },
  deleteIndex: (para) => {
    return Vue.resource(apiUrl + `index_plans/index/${para.id}?project=${para.project}&model=${para.model}`).delete()
  },
  fetchIndexGraph: (para) => {
    return Vue.resource(apiUrl + 'index_plans/index_graph').get(para)
  }
}
