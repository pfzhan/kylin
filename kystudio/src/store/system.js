import api from './../service/api'
import * as types from './types'
import { getProperty } from '../util/business'
export default {
  state: {
    authentication: null,
    serverConfig: null,
    serverEnvironment: null,
    serverAboutKap: null,
    timeZone: ''
  },
  mutations: {
    [types.SAVE_AUTHENTICATION]: function (state, result) {
      state.authentication = result.authentication
    },
    [types.SAVE_ENV]: function (state, result) {
      state.serverEnvironment = result.env
    },
    [types.SAVE_CONF]: function (state, result) {
      state.serverConfig = result.conf
    },
    [types.GET_TIMEZONE]: function (state, name) {
      if (!state.timeZone) {
        state.timeZone = getProperty(name, state.serverConfig)
      } else {
        return state.timeZone
      }
    },
    [types.GET_CONF_BY_NAME]: function (state, {name, key}) {
      // if (!state[key]) {
      state[key] = getProperty(name, state.serverConfig)
      // } else {
      return state[key]
      // }
    },
    [types.GET_ABOUT]: function (state, result) {
      state.serverAboutKap = result.list
    }
  },
  actions: {
    [types.LOAD_AUTHENTICATION]: function ({ commit }) {
      api.system.getAuthentication().then((response) => {
        commit(types.SAVE_AUTHENTICATION, { authentication: response.data })
      })
    },
    [types.GET_ENV]: function ({ commit }) {
      return api.system.getEnv().then((response) => {
        commit(types.SAVE_ENV, { env: response.data.data })
      })
    },
    [types.GET_CONF]: function ({ commit }) {
      return api.system.getConfig().then((response) => {
        commit(types.SAVE_CONF, { conf: response.data.data })
        commit(types.GET_CONF_BY_NAME, {name: 'kylin.web.timezone', key: 'timeZone'})
        commit(types.GET_CONF_BY_NAME, {name: 'kap.kyaccount.username', key: 'kyAccount'})
        commit(types.GET_CONF_BY_NAME, {name: 'kap.license.statement', key: 'statement'})
        commit(types.GET_CONF_BY_NAME, {name: 'kap.web.limitfact', key: 'limitfact'})
      })
    },
    [types.RELOAD_METADATA]: function ({ commit }) {
      return api.system.reloadMetadata()
    },
    [types.BACKUP_METADATA]: function ({ commit }) {
      return api.system.backupMetadata()
    },
    [types.UPDATE_CONFIG]: function ({ commit }, config) {
      return api.system.updateConfig(config)
    },
    [types.GET_ABOUTKAP]: function ({ commit }) {
      return api.system.getAboutKap().then((response) => {
        // console.log('response ::', response.data)
        commit(types.GET_ABOUT, { list: response.data.data })
      })
    },
    [types.GET_KYBOT_UPLOAD]: function ({ commit }, {startTime, endTime}) {
      return api.system.getKybotUpload(startTime, endTime)
    },
    [types.GET_JOB_KYBOT]: function ({commit}, target) {
      return api.system.getJobKtbot(target).then((response) => {
        return response
      })
    },
    [types.GET_KYBOT_DUMP]: function ({ commit }, {startTime, endTime}) {
      return api.system.getKybotDump(startTime, endTime)
    }
  },
  getters: {}
}

