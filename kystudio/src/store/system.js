import api from './../service/api'
import * as types from './types'
import { getProperty } from '../util/business'
export default {
  state: {
    needReset: false,
    authentication: null,
    serverConfig: null,
    serverEnvironment: null,
    serverAboutKap: null,
    timeZone: '',
    securityProfile: 'testing',
    limitlookup: 'true',
    strategy: 'default',
    showHtrace: false,
    filterUserName: ''// group页面 选择用户组件使用
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
      if (name === 'kylin.web.timezone') {
        localStorage.setItem('GlobalSeverTimeZone', state[key])
      }
      return state[key]
      // }
    },
    [types.GET_ABOUT]: function (state, result) {
      state.serverAboutKap = result.list
    }
  },
  actions: {
    [types.LOAD_AUTHENTICATION]: function ({ commit }) {
      /* api.system.getAuthentication().then((response) => {
        commit(types.SAVE_AUTHENTICATION, { authentication: response.data })
      }) */
      return new Promise((resolve, reject) => {
        api.system.getAuthentication().then((response) => {
          commit(types.SAVE_AUTHENTICATION, { authentication: response.data })
          resolve(response.data)
        }, () => {
          reject()
        })
      })
    },
    [types.GET_ENV]: function ({ commit }) {
      return api.system.getEnv().then((response) => {
        commit(types.SAVE_ENV, { env: response.data.data })
      })
    },
    [types.GET_CONF]: function ({ commit }) {
      /* return api.system.getConfig().then((response) => {
        commit(types.SAVE_CONF, { conf: response.data.data })
        commit(types.GET_CONF_BY_NAME, {name: 'kylin.web.timezone', key: 'timeZone'})
        commit(types.GET_CONF_BY_NAME, {name: 'kap.kyaccount.username', key: 'kyAccount'})
        commit(types.GET_CONF_BY_NAME, {name: 'kap.license.statement', key: 'statement'})
        commit(types.GET_CONF_BY_NAME, {name: 'kap.web.hide-feature.limited-lookup', key: 'limitlookup'})
        commit(types.GET_CONF_BY_NAME, {name: 'kylin.security.profile', key: 'securityProfile'})
        commit(types.GET_CONF_BY_NAME, {name: 'kap.smart.conf.aggGroup.strategy', key: 'strategy'})
      }) */

      return new Promise((resolve, reject) => {
        api.system.getConfig().then((response) => {
          commit(types.SAVE_CONF, { conf: response.data.data })
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.web.timezone', key: 'timeZone'})
          commit(types.GET_CONF_BY_NAME, {name: 'kap.kyaccount.username', key: 'kyAccount'})
          commit(types.GET_CONF_BY_NAME, {name: 'kap.license.statement', key: 'statement'})
          commit(types.GET_CONF_BY_NAME, {name: 'kap.web.hide-feature.limited-lookup', key: 'limitlookup'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.security.profile', key: 'securityProfile'})
          commit(types.GET_CONF_BY_NAME, {name: 'kap.smart.conf.aggGroup.strategy', key: 'strategy'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.htrace.show-gui-trace-toggle', key: 'showHtrace'})
          resolve(response.data.data)
        }, () => {
          reject()
        })
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
    },
    [types.SAVE_LICENSE_CONTENT]: function ({ commit }, license) {
      return api.system.saveLicenseContent(license).then((response) => {
        commit(types.GET_ABOUT, { list: response.data.data })
      })
    },
    [types.SAVE_LICENSE_FILE]: function ({ commit }, formData) {
      return api.system.saveLicenseFile().then((response) => {
        commit(types.GET_ABOUT, { list: response.data.data })
      })
    },
    [types.TRIAL_LICENSE_FILE]: function ({ commit }, user) {
      return api.system.trialLicenseFile(user)
    }
  },
  getters: {}
}

