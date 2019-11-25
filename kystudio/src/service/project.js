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
    return Vue.resource(apiUrl + 'projects').update({ former_project_name: project.name, project_desc_data: project.desc })
  },
  saveProject: (projectDesc) => {
    return Vue.resource(apiUrl + 'projects').save(projectDesc)
  },
  addProjectAccess: (accessData, projectId) => {
    return Vue.resource(apiUrl + 'access/batch/ProjectInstance/' + projectId).save(accessData)
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
  delProjectAccess: (projectId, aid, userName, principal) => {
    return Vue.resource(apiUrl + 'access/ProjectInstance/' + projectId).delete({
      access_entry_id: aid,
      sid: userName,
      principal: principal
    })
  },
  submitAccessData: (projectName, userType, roleOrName, accessData) => {
    return Vue.resource(apiUrl + `acl/sid/${userType}/${roleOrName}?project=${projectName}`).update(accessData)
  },
  saveProjectFilter: (filterData) => {
    return Vue.resource(apiUrl + 'ext_filter/save_ext_filter').save(filterData)
  },
  getProjectFilter: (project) => {
    return Vue.resource(apiUrl + 'ext_filter').get({
      project: project
    })
  },
  delProjectFilter: (project, filterName) => {
    return Vue.resource(apiUrl + 'ext_filter/' + filterName + '/' + project).delete()
  },
  updateProjectFilter: (filterData) => {
    return Vue.resource(apiUrl + 'ext_filter/update_ext_filter').update(filterData)
  },
  backupProject: (project) => {
    return Vue.resource(apiUrl + 'projects/' + project.name + '/backup').save()
  },
  accessAvailableUserOrGroup: (sidType, uuid, data) => {
    return Vue.resource(apiUrl + 'access/available/' + sidType + '/' + uuid).get(data)
  },
  getQuotaInfo: (para) => {
    return Vue.resource(apiUrl + 'projects/' + para.project + '/storage_volume_info').get()
  },
  clearTrash: (para) => {
    return Vue.resource(apiUrl + 'projects/' + para.project + '/storage').update()
  },
  fetchProjectSettings: (project) => {
    return Vue.resource(apiUrl + 'projects/' + project + '/project_config').get()
  },
  updateProjectGeneralInfo (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/project_general_info').update(body)
  },
  updateSegmentConfig (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/segment_config').update(body)
  },
  updatePushdownConfig (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/push_down_config').update(body)
  },
  updateStorageQuota (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/garbage_cleanup_config').update(body)
  },
  updateAccelerationSettings (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/query_accelerate_threshold').update(body)
  },
  updateJobAlertSettings (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/job_notification_config').update(body)
  },
  updateProjectDatasource (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/source_type').update(body)
  },
  resetConfig (para) {
    return Vue.resource(apiUrl + 'projects/' + para.project + '/project_config').update({reset_item: para.reset_item})
  },
  updateDefaultDBSettings (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/default_database').update({default_database: body.default_database})
  },
  updateYarnQueue (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/yarn_queue').update(body)
  }
}
