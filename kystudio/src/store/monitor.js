import api from './../service/api'
import * as types from './types'
export default {
  state: {
    jobsList: [],
    slowQueries: [],
    pushdownQueries: [],
    totalJobs: 0,
    totalSlowQueries: 0,
    totalPushDownQueries: 0,
    filter: {
      timeFilter: 1,
      jobName: '',
      sortby: 'last_modify',
      status: []
    }
  },
  mutations: {
    [types.SAVE_JOBS_LIST]: function (state, { list, total }) {
      state.jobsList = list
      state.totalJobs = total
    },
    [types.SAVE_SLOW_QUERIES]: function (state, { list, total }) {
      state.slowQueries = list
      state.totalSlowQueries = total
    },
    [types.SAVE_PUSHDOWN_QUERIES]: function (state, { list, total }) {
      state.pushdownQueries = list
      state.totalPushDownQueries = total
    },
    [types.RESET_MONITOR_STATE]: function (state) {
      state.jobsList.splice(0, state.jobsList.length)
      state.slowQueries.splice(0, state.slowQueries.length)
      state.pushdownQueries.splice(0, state.pushdownQueries.length)
      state.totalJobs = 0
      state.totalSlowQueries = 0
      state.totalPushDownQueries = 0
      state.filter.timeFilter = 1
      state.filter.jobName = ''
      state.filter.sortby = 'last_modify'
      state.filter.status = []
    }
  },
  actions: {
    [types.LOAD_JOBS_LIST]: function ({ commit, state }, params) {
      return api.monitor.getJobsList(params).then((response) => {
        commit(types.SAVE_JOBS_LIST, { list: response.data.data.jobs, total: response.data.data.size })
      }, () => {
        state.jobsList = []
        state.totalJobs = 0
      })
    },
    [types.LOAD_SLOW_QUERIES]: function ({ commit }, para) {
      api.monitor.getSlowQueries(para).then((response) => {
        commit(types.SAVE_SLOW_QUERIES, { list: response.data.data.badQueries, total: response.data.data.size })
      })
    },
    [types.LOAD_PUSHDOWN_QUERIES]: function ({ commit }, para) {
      api.monitor.getPushDownQueries(para).then((response) => {
        commit(types.SAVE_PUSHDOWN_QUERIES, { list: response.data.data.badQueries, total: response.data.data.size })
      })
    },
    [types.EXPORT_PUSHDOWN]: function ({ commit }, para) {
      return api.monitor.exportPushDownQueries(para)
    },
    [types.LOAD_STEP_OUTPUTS]: function ({ commit }, stepDetail) {
      return api.monitor.getStepOutputs(stepDetail)
    },
    [types.RESUME_JOB]: function ({ commit }, jobId) {
      return api.monitor.resumeJob(jobId)
    },
    [types.CANCEL_JOB]: function ({ commit }, jobId) {
      return api.monitor.cancelJob(jobId)
    },
    [types.PAUSE_JOB]: function ({ commit }, jobId) {
      return api.monitor.pauseJob(jobId)
    },
    [types.REMOVE_JOB]: function ({ commit }, jobId) {
      return api.monitor.removeJob(jobId)
    }
  },
  getters: {}
}

