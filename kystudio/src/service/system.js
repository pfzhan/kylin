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
  // 获取节点的一些固定配置信息
  getInstanceConfig: () => {
    return Vue.resource(apiUrl + 'admin/instance_info').get()
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
    return Vue.resource(apiUrl + 'system/license').get()
  },
  getKybotDump: (para) => {
    return Vue.resource(apiUrl + 'kybot/dump_remote').get(para)
  },
  getKybotUpload: (para) => {
    return Vue.resource(apiUrl + 'kybot/upload_remote').get(para)
  },
  saveLicenseContent: (license) => {
    return Vue.resource(apiUrl + 'system/license/content').save(license)
  },
  saveLicenseFile: () => {
    return Vue.resource(apiUrl + 'system/license/file').save({})
  },
  trialLicenseFile: (para) => {
    return Vue.resource(apiUrl + 'system/license/trial').save(para)
  },
  getServiceState: () => {
    return Vue.resource(apiUrl + 'service_discovery/state/all').get()
  },
  getCanaryReport: (para) => {
    return Vue.resource(apiUrl + 'canary/report').get(para)
  }
}
