import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getJobsList: (params) => {
    return Vue.resource(apiUrl + 'jobs{?status}').get(params)
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
  resumeJob: (jobID) => {
    return Vue.resource(apiUrl + 'jobs/' + jobID + '/resume').update({})
  },
  cancelJob: (jobID) => {
    return Vue.resource(apiUrl + 'jobs/' + jobID + '/cancel').update({})
  },
  pauseJob: (jobID) => {
    return Vue.resource(apiUrl + 'jobs/' + jobID + '/pause').update({})
  },
  removeJob: (jobID) => {
    return Vue.resource(apiUrl + 'jobs/' + jobID + '/drop').delete({})
  }
}
