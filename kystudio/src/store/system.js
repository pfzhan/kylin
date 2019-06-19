import api from './../service/api'
import * as types from './types'
import { getProperty } from '../util/business'
export default {
  state: {
    needReset: false,
    authentication: null,
    adminConfig: null,
    serverConfig: null,
    serverEnvironment: null,
    serviceState: [],
    serverAboutKap: {},
    canaryReport: {},
    timeZone: '',
    securityProfile: 'testing',
    limitlookup: 'true',
    strategy: 'default',
    showHtrace: false,
    hiddenRaw: true,
    hiddenExtendedColumn: true,
    storage: 2,
    engine: 2,
    allowAdminExport: 'true',
    allowNotAdminExport: 'true',
    filterUserName: '', // group页面 选择用户组件使用
    canaryReloadTimer: 15,
    sourceDefault: 0,
    lang: 'en',
    platform: '',
    guideConfig: {
      guideType: '',
      guideModeCheckDialog: false,
      globalMouseDrag: false,
      globalMaskVisible: false,
      globalMouseVisible: false,
      globalMouseClick: false,
      mousePos: {
        x: 0,
        y: 0
      },
      targetList: {}
    }
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
    [types.SAVE_ADMIN_CONF]: function (state, result) {
      state.adminConfig = result.conf
    },
    [types.GET_CONF_BY_NAME]: function (state, {name, key}) {
      state[key] = getProperty(name, state.serverConfig)
      return state[key]
    },
    [types.SAVE_INSTANCE_CONF_BY_NAME]: function (state, {key, val}) {
      state[key] = val
      if (key === 'timeZone') {
        localStorage.setItem('GlobalSeverTimeZone', state[key])
      }
    },
    [types.GET_ABOUT]: function (state, result) {
      state.serverAboutKap = result.list.data
      let version = /^[^/d]+?(\d).*$/.exec(result.list.data['kap.version'])
      state.serverAboutKap['version'] = version && version.length >= 2 ? version[1] : '2'
      state.serverAboutKap['msg'] = result.list.msg
      state.serverAboutKap['code'] = result.list.code
    },
    [types.SAVE_SERVICE_STATE]: function (state, result) {
      state.serviceState = result.list
    },
    [types.SAVE_CANARY_REPORT]: function (state, result) {
      state.canaryReport = result.list
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
          commit(types.INIT_SPEC)
          resolve(response.data)
        }, () => {
          reject()
        })
      })
    },
    [types.GET_ADMIN_CONFIG]: function ({ commit }) {
      return api.system.getConfig().then((response) => {
        commit(types.SAVE_ADMIN_CONF, { conf: response.data.data })
        return response
      })
    },
    [types.GET_ENV]: function ({ commit }) {
      return api.system.getEnv().then((response) => {
        commit(types.SAVE_ENV, { env: response.data.data })
        return response
      })
    },
    [types.GET_CONF]: function ({ commit }) {
      return new Promise((resolve, reject) => {
        api.system.getPublicConfig().then((response) => {
          commit(types.SAVE_CONF, { conf: response.data.data })
          // commit(types.GET_CONF_BY_NAME, {name: 'kylin.web.timezone', key: 'timeZone'})
          // commit(types.GET_CONF_BY_NAME, {name: 'kap.web.hide-feature.limited-lookup', key: 'limitlookup'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.security.profile', key: 'securityProfile'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.htrace.show-gui-trace-toggle', key: 'showHtrace'})
          commit(types.GET_CONF_BY_NAME, {name: 'kap.web.hide-feature.raw-measure', key: 'hiddenRaw'})
          commit(types.GET_CONF_BY_NAME, {name: 'kap.web.hide-feature.extendedcolumn-measure', key: 'hiddenExtendedColumn'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.engine.default', key: 'engine'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.storage.default', key: 'storage'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.web.export-allow-admin', key: 'allowAdminExport'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.web.export-allow-other', key: 'allowNotAdminExport'})
          commit(types.GET_CONF_BY_NAME, {name: 'kap.canary.default-canaries-period-min', key: 'canaryReloadTimer'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.source.default', key: 'sourceDefault'})
          resolve(response)
        }, () => {
          reject()
        })
      })
    },
    [types.GET_INSTANCE_CONF]: function ({ commit }) {
      return new Promise((resolve, reject) => {
        api.system.getInstanceConfig().then((response) => {
          try {
            commit(types.SAVE_INSTANCE_CONF_BY_NAME, {key: 'timeZone', val: response.data.data['instance.timezone']})
            resolve(response)
          } catch (e) {
            reject(e)
          }
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
        commit(types.GET_ABOUT, { list: response.data })
      })
    },
    [types.GET_KYBOT_UPLOAD]: function ({ commit }, para) {
      return api.system.getKybotUpload(para)
    },
    [types.GET_JOB_KYBOT]: function ({commit}, para) {
      return api.system.getKybotUpload(para).then((response) => {
        return response
      })
    },
    [types.GET_KYBOT_DUMP]: function ({ commit }, para) {
      return api.system.getKybotDump(para)
    },
    [types.SAVE_LICENSE_CONTENT]: function ({ commit }, license) {
      return api.system.saveLicenseContent(license).then((response) => {
        commit(types.GET_ABOUT, { list: response.data })
        return response
      })
    },
    [types.SAVE_LICENSE_FILE]: function ({ commit }, formData) {
      return api.system.saveLicenseFile().then((response) => {
        commit(types.GET_ABOUT, { list: response.data })
      })
    },
    [types.TRIAL_LICENSE_FILE]: function ({ commit }, user) {
      return api.system.trialLicenseFile(user)
    },
    [types.GET_SERVICE_STATE]: function ({ commit }) {
      return api.system.getServiceState().then((response) => {
        commit(types.SAVE_SERVICE_STATE, { list: response.data.data })
      })
    },
    [types.GET_CANARY_REPORT]: function ({ commit }, para) {
      return api.system.getCanaryReport(para).then((response) => {
        commit(types.SAVE_CANARY_REPORT, { list: response.data.data })
      })
    }
  },
  getters: {
    isTestingSecurityProfile (state) {
      return state.securityProfile === 'testing'
    },
    isGuideMode (state) {
      return state.guideConfig.globalMaskVisible
    }
  }
}

