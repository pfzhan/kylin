import Vue from 'vue'
import VueResource from 'vue-resource'
import projectApi from './project'
import modelApi from './model'
import cubeApi from './cube'

Vue.use(VueResource)

export default {
  project: projectApi,
  model: modelApi,
  cube: cubeApi
}
