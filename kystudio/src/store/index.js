
import Vuex from 'vuex'
import Vue from 'vue'
Vue.use(Vuex)
import model from './model'
import project from './project'
import cube from './cube'
export default new Vuex.Store({
  modules: {
    model: model,
    project: project,
    cube: cube
  }
})
