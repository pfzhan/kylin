import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getProjectList: (params) => {
    return Vue.resource(apiUrl + 'projects').get(params)
  },
  deleteProject: (projectName) => {
    return Vue.resource(apiUrl + 'projects/' + projectName).delete()
  },
  updateProject: (project) => {
    return Vue.resource(apiUrl + 'projects').update({ formerProjectName: project.name, projectDescData: project.desc })
  },
  saveProject: (projectDesc) => {
    return Vue.resource(apiUrl + 'projects').save({projectDescData: projectDesc})
  },
  addProjectAccess: (accessData, projectId) => {
    return Vue.resource(apiUrl + 'access/ProjectInstance/' + projectId).save(accessData)
  },
  editProjectAccess: (accessData, projectId) => {
    return Vue.resource(apiUrl + 'access/ProjectInstance/' + projectId).update(accessData)
  },
  getProjectAccess: (projectId, data) => {
    return Vue.resource(apiUrl + 'access/ProjectInstance/' + projectId).get(data)
  },
  getProjectEndAccess: (projectId) => {
    return Vue.resource(apiUrl + 'access/all/ProjectInstance/' + projectId).get()
  },
  delProjectAccess: (projectId, aid, userName) => {
    return Vue.resource(apiUrl + 'access/ProjectInstance/' + projectId).delete({
      accessEntryId: aid,
      sid: userName
    })
  },
  saveProjectFilter: (filterData) => {
    return Vue.resource(apiUrl + 'extFilter/saveExtFilter').save(filterData)
  },
  getProjectFilter: (project) => {
    return Vue.resource(apiUrl + 'extFilter').get({
      project: project
    })
  },
  delProjectFilter: (project, filterName) => {
    return Vue.resource(apiUrl + 'extFilter/' + filterName + '/' + project).delete()
  },
  updateProjectFilter: (filterData) => {
    return Vue.resource(apiUrl + 'extFilter/updateExtFilter').update(filterData)
  },
  backupProject: (project) => {
    return Vue.resource(apiUrl + 'projects/backup/' + project.name).save()
  },
  accessAvailableUserOrGroup: (sidType, uuid, data) => {
    return Vue.resource(apiUrl + 'access/available/' + sidType + '/' + uuid).get(data)
  },
  getQuotaInfo: (para) => {
    return Vue.resource(apiUrl + 'projects/storage_volume_info').get(para)
  },
  clearTrash: (para) => {
    return Vue.resource(apiUrl + 'projects/storage?project=' + para.project).update()
  },
  fetchProjectSettings: (project) => {
    return Vue.resource(apiUrl + 'projects/project_config').get({ project })
  },
  updateProjectGeneralInfo (body) {
    return Vue.resource(apiUrl + 'projects/project_general_info').update(body)
  },
  updateSegmentConfig (body) {
    return Vue.resource(apiUrl + 'projects/segment_config').update(body)
  },
  updatePushdownConfig (body) {
    return Vue.resource(apiUrl + 'projects/push_down_config').update(body)
  },
  updateStorageQuota (body) {
    return Vue.resource(apiUrl + 'projects/storage_quota').update(body)
  },
  updateAccelerationSettings (body) {
    return Vue.resource(apiUrl + 'projects/query_accelerate_threshold').update(body)
  },
  updateJobAlertSettings (body) {
    return Vue.resource(apiUrl + 'projects/job_notification_config').update(body)
  }
}
