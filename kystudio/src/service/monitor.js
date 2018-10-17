import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getJobsList: (params) => {
    return Vue.resource(apiUrl + 'jobs{?status}').get(params)
  },
  getJobDetail: (para) => {
    return Vue.resource(apiUrl + 'jobs/detail').get(para)
  },
  getSlowQueries: (para) => {
    return Vue.resource(apiUrl + 'diag/slow_query').get(para.page)
  },
  getPushDownQueries: (para) => {
    return Vue.resource(apiUrl + 'diag/push_down').get(para.page)
  },
  exportPushDownQueries: (para) => {
    return Vue.resource(apiUrl + 'diag/export/push_down').save(para)
  },
  getStepOutputs: (stepDetail) => {
    return Vue.resource(apiUrl + 'jobs/' + stepDetail.jobID + '/steps/' + stepDetail.stepID + '/output').get()
  },
  resumeJob: (para) => {
    return Vue.resource(apiUrl + 'jobs/status').update(para)
  },
  cancelJob: (para) => {
    return Vue.resource(apiUrl + 'jobs/status').update(para)
  },
  pauseJob: (para) => {
    return Vue.resource(apiUrl + 'jobs/status').update(para)
  },
  removeJob: (para) => {
    return Vue.resource(apiUrl + 'jobs/' + para.project + '/' + para.jobId).delete()
  }
}
