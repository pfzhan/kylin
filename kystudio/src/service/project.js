import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getProjectList: (params) => {
    return Vue.resource(apiUrl + 'projects/readable').get(params)
  }
}
