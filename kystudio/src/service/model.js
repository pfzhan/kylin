import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getModelList: (params) => {
    return Vue.resource(apiUrl + 'models').get(params)
  },
  measureDimensionSuggestion: (params) => {
    return Vue.resource(apiUrl + 'models/' + params.project + '/table_suggestions').get(params)
  },
  getModelByModelName: (para) => {
    return Vue.resource(apiUrl + 'model_desc/' + para.project + '/' + para.modelName).get()
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
  diagnoseList: (project, offSet, pageSize) => {
    return Vue.resource(apiUrl + 'models/get_all_stats').get({
      projectName: project,
      pageOffset: offSet,
      pageSize: pageSize
    })
  },
  checkModelName: (para) => {
    return Vue.resource(apiUrl + 'models?modelName=' + para.modelName + '&projectName=' + para.project).get()
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
  }
}
