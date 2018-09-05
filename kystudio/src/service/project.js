import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getProjectList: (params) => {
    return Vue.resource(apiUrl + 'projects').get(params)
  },
  deleteProject: (projectName) => {
    return Vue.resource(apiUrl + 'projects/' + projectName).remove({})
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
    return Vue.resource(apiUrl + 'metastore/backup?project=' + project.name).save()
  },
  accessAvailableUserOrGroup: (sidType, uuid, data) => {
    return Vue.resource(apiUrl + 'access/available/' + sidType + '/' + uuid).get(data)
  }
}
