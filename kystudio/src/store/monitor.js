import api from './../service/api'
import * as types from './types'
import { getAvailableOptions } from '../util/specParser'
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
    [types.LOAD_WAITTING_JOB_MODELS]: function ({ commit }, para) {
      return api.monitor.losdWaittingJobModels(para)
    },
    [types.LOAD_WAITTING_JOBS_BY_MODEL]: function ({ commit }, para) {
      return api.monitor.laodWaittingJobsByModel(para)
    },
    [types.EXPORT_PUSHDOWN]: function ({ commit }, para) {
      return api.monitor.exportPushDownQueries(para)
    },
    [types.LOAD_STEP_OUTPUTS]: function ({ commit }, para) {
      return api.monitor.getStepOutputs(para)
    },
    [types.RESUME_JOB]: function ({ commit }, para) {
      return api.monitor.resumeJob(para)
    },
    [types.RESTART_JOB]: function ({ commit }, para) {
      return api.monitor.restartJob(para)
    },
    [types.PAUSE_JOB]: function ({ commit }, para) {
      return api.monitor.pauseJob(para)
    },
    [types.REMOVE_JOB]: function ({ commit }, para) {
      return api.monitor.removeJob(para)
    },
    [types.ROMOVE_JOB_FOR_ALL]: function ({ commit }, para) {
      return api.monitor.removeJobForAll(para)
    },
    [types.LOAD_DASHBOARD_JOB_INFO]: function ({ commit }, para) {
      return api.monitor.loadDashboardJobInfo(para)
    },
    [types.LOAD_JOB_CHART_DATA]: function ({ commit }, para) {
      return api.monitor.loadJobChartData(para)
    },
    [types.LOAD_JOB_BULID_CHART_DATA]: function ({ commit }, para) {
      return api.monitor.loadJobBulidChartData(para)
    }
  },
  getters: {
    monitorActions (state, getters, rootState, rootGetters) {
      const groupRole = rootGetters.userAuthorities
      const projectRole = rootState.user.currentUserAccess

      return getAvailableOptions('monitorActions', { groupRole, projectRole })
    }
  }
}

