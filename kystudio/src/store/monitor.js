import api from './../service/api'
import * as types from './types'
export default {
  state: {
  },
  actions: {
    [types.LOAD_JOBS_LIST]: function ({ commit, state }, params) {
      return api.monitor.getJobsList(params)
    },
    [types.GET_JOB_DETAIL]: function ({ commit }, para) {
      return api.monitor.getJobDetail(para)
    },
    [types.EXPORT_PUSHDOWN]: function ({ commit }, para) {
      return api.monitor.exportPushDownQueries(para)
    },
    [types.LOAD_STEP_OUTPUTS]: function ({ commit }, stepDetail) {
      return api.monitor.getStepOutputs(stepDetail)
    },
    [types.RESUME_JOB]: function ({ commit }, para) {
      return api.monitor.resumeJob(para)
    },
    [types.CANCEL_JOB]: function ({ commit }, para) {
      return api.monitor.cancelJob(para)
    },
    [types.PAUSE_JOB]: function ({ commit }, para) {
      return api.monitor.pauseJob(para)
    },
    [types.REMOVE_JOB]: function ({ commit }, para) {
      return api.monitor.removeJob(para)
    }
  },
  getters: {}
}

