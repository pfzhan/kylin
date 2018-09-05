import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'
import { aggregateTree } from '../../mock/aggregateIndex'

Vue.use(VueResource)

export default {
  getModelList: (params) => {
    return Vue.resource(apiUrl + 'models').get(params)
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
    return Vue.resource(apiUrl + 'models/' + para.oldName + '/clone').update(para.data)
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
  // newten
  getAggregateIndex: params => {
    // Vue.resource(`${apiUrl}/models/agg_indexs`).get(params)
    return aggregateTree
  }
}
