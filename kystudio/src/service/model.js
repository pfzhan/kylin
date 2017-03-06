import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getModelList: (params) => {
    return Vue.resource(apiUrl + 'models').get(params)
  }
}
