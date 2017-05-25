import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getJobsList: (params) => {
    return Vue.resource(apiUrl + 'jobs{?status}').get(params)
  },
  getSlowQueries: (para) => {
    return Vue.resource(apiUrl + 'diag/sql').get(para.page)
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
    return Vue.resource(apiUrl + 'jobs/' + jobID).delete({})
  }
}
