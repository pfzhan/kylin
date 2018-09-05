import api from './../service/api'
import * as types from './types'
export default {
  state: {
    queryHistory: [],
    totalQueries: 0,
    jobsList: [],
    totalJobs: 0,
    selectedQuery: 0
  },
  mutations: {
    [types.AUTO_JOBS_LIST]: function (state, { list, total }) {
      state.jobsList = list
      state.totalJobs = total
    },
    [types.SAVE_QUERY_HISTORY]: function (state, { list, total }) {
      state.queryHistory = list
      state.totalQueries = total
    }
  },
  actions: {
    [types.LOAD_QUERY_HISTORY]: function ({ commit }, para) {
      return api.auto.getQueryHistory(para).then((response) => {
        commit(types.SAVE_QUERY_HISTORY, { list: response.data.data.entries, total: response.data.data.entries.length })
        return response
      })
    },
    [types.AUTO_CHECK_SQL]: function ({ commit }, para) {
      return api.auto.checkSql(para)
    },
    [types.AUTO_QUERY_PATTERNS]: function ({ commit }, para) {
      return api.auto.queryPatterns(para)
    },
    [types.AUTO_MUTIL_MODELS]: function ({ commit }, para) {
      return api.auto.mutilModels(para)
    },
    [types.AUTO_SAVE_MODELS]: function ({ commit }, para) {
      return api.auto.saveModels(para)
    },
    [types.AUTO_MODELING_JOBS]: function ({ commit, state }, para) {
      return api.auto.getModelingJobs(para).then((response) => {
        commit(types.AUTO_JOBS_LIST, { list: response.data.data.jobs, total: response.data.data.size })
      }, () => {
        state.jobsList = []
        state.totalJobs = 0
      })
    },
    [types.AUTO_BUILD_MODELS]: function ({ commit }, para) {
      return api.auto.buildModels(para)
    }
  },
  getters: {}
}

