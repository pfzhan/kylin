import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getEnv: () => {
    return Vue.resource(apiUrl + 'admin/env').get()
  },
  getConfig: () => {
    return Vue.resource(apiUrl + 'admin/config').get()
  },
  getPublicConfig: () => {
    return Vue.resource(apiUrl + 'admin/public_config').get()
  },
  getAuthentication: () => {
    return Vue.resource(apiUrl + 'user/authentication').get()
  },
  reloadMetadata: () => {
    return Vue.resource(apiUrl + 'cache/announce/all/all/update').update()
  },
  backupMetadata: () => {
    return Vue.resource(apiUrl + 'metastore/backup').save()
  },
  updateConfig: (config) => {
    return Vue.resource(apiUrl + 'admin/config').update(config)
  },
  getAboutKap: () => {
    return Vue.resource(apiUrl + 'kap/system/license').get()
  },
  getKybotDump: (startTime, endTime) => {
    return Vue.resource(apiUrl + 'kybot/dump').get({
      startTime: startTime,
      endTime: endTime,
      currentTime: +(new Date())
    })
  },
  getKybotUpload: (startTime, endTime) => {
    return Vue.resource(apiUrl + 'kybot/upload').get({
      startTime: startTime,
      endTime: endTime,
      currentTime: +(new Date())
    })
  },
  getJobKtbot: (target) => {
    return Vue.resource(apiUrl + 'kybot/upload?target=' + target).get()
  },
  saveLicenseContent: (license) => {
    return Vue.resource(apiUrl + 'kap/system/license/content').save(license)
  },
  saveLicenseFile: () => {
    return Vue.resource(apiUrl + 'kap/system/license/file').save({})
  },
  trialLicenseFile: (para) => {
    return Vue.resource(apiUrl + 'kap/system/license/trial').save(para)
  }
}
