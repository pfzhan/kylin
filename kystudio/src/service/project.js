import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getProjectList: (params) => {
    return Vue.resource(apiUrl + 'projects/readable').get(params)
  },
  deleteProject: (projectName) => {
    return Vue.resource(apiUrl + 'projects/' + projectName).remove({})
  },
  updateProject: (project) => {
    return Vue.resource(apiUrl + 'projects').update({}, { formerProjectName: project.name, projectDescData: project.desc })
  },
  saveProject: (project) => {
    return Vue.resource(apiUrl + 'projects').save({}, {projectDescData: project})
  }
}
