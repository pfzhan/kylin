import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getDefaults: () => {
    return Vue.resource(apiUrl + 'config/defaults').get()
  },
  hiddenMeasure: (feature) => {
    return Vue.resource(apiUrl + 'config/hidden_feature').get(feature)
  }
}
