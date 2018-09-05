import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getQueryHistory: (para) => {
    return Vue.resource(apiUrl + 'smart/lab/' + para.project + '/bad_query_history').get()
  },
  checkSql: (para) => {
    return Vue.resource(apiUrl + 'smart/lab/validate_sql').save(para)
  },
  queryPatterns: (para) => {
    return Vue.resource(apiUrl + 'smart/lab/query_patterns').save(para)
  },
  mutilModels: (para) => {
    return Vue.resource(apiUrl + 'smart/lab/multiple_model?project=' + para.project).save(para.para)
  },
  saveModels: (para) => {
    return Vue.resource(apiUrl + 'smart/lab/save_multiple_model').save(para)
  },
  getModelingJobs: (para) => {
    return Vue.resource(apiUrl + 'smart/lab/' + para.project + '/auto_modeling_jobs').get(para.filter)
  },
  buildModels: (para) => {
    return Vue.resource(apiUrl + 'smart/lab/build_multiple_model').save(para)
  }
}

