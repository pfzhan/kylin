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
  getModelByModelName: (modelname) => {
    return Vue.resource(apiUrl + 'model_desc/' + modelname).get()
  },
  deleteModel: (modelname) => {
    return Vue.resource(apiUrl + 'models/' + modelname).delete()
  },
  collectStats: (params) => {
    return Vue.resource(apiUrl + 'models/' + params.project + '/' + params.modelname + '/stats').save(params.data)
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
  checkModelName: (modelName) => {
    return Vue.resource(apiUrl + 'models?modelName=' + modelName).get()
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
  }
}
