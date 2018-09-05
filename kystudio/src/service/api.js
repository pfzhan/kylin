import Vue from 'vue'
import VueResource from 'vue-resource'
import projectApi from './project'
import modelApi from './model'
import cubeApi from './cube'
import configApi from './config'
import kafkaApi from './kafka'
import userApi from './user'
import systemApi from './system'
import datasourceApi from './datasource'
import monitorApi from './monitor'
import kybot from './kybot'
import autoApi from './auto'
// console.log(base64)
Vue.use(VueResource)
export default {
  project: projectApi,
  model: modelApi,
  cube: cubeApi,
  config: configApi,
  kafka: kafkaApi,
  user: userApi,
  system: systemApi,
  datasource: datasourceApi,
  monitor: monitorApi,
  kybot: kybot,
  auto: autoApi
}
