import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getCubesList: (params) => {
    return Vue.resource(apiUrl + 'cubes').get(params)
  }
}
