import api from './../service/api'
import * as types from './types'
export default {
  state: {
    modelsList: [],
    modelsDianoseList: [],
    modelsTotal: 0,
    modelEditCache: {}
  },
  mutations: {
    [types.SAVE_MODEL_LIST]: function (state, result) {
      state.modelsList = result.list
      state.modelsTotal = result.total
    },
    [types.CACHE_MODEL_EDIT]: function (state, result) {
      var project = result.project
      state.modelEditCache[project + '$' + result.name] = {
        data: result,
        editable: false
      }
    },
    [types.CACHE_MODEL_DIANOSELIST]: function (state, {data}) {
      state.modelsDianoseList = data
    }
  },
  actions: {
    [types.LOAD_MODEL_LIST]: function ({ commit }, para) {
      api.model.getModelList(para).then((response) => {
        commit(types.SAVE_MODEL_LIST, { list: response.data.data.models, total: response.data.data.size })
      })
    },
    [types.SUGGEST_DIMENSION_MEASURE]: function ({ commit }, para) {
      return api.model.measureDimensionSuggestion(para)
    },
    [types.LOAD_MODEL_INFO]: function ({ commit }, para) {
      return api.model.getModelByModelName(para).then((response) => {
        commit(types.CACHE_MODEL_EDIT, response.data.data.draft || response.data.data.model)
        return response
      })
    },
    [types.DELETE_MODEL]: function ({ commit }, para) {
      return api.model.deleteModel(para)
    },
    [types.CLONE_MODEL]: function ({ commit }, para) {
      return api.model.cloneModel(para)
    },
    [types.SAVE_MODEL]: function ({ commit }, para) {
      return api.model.saveModel(para)
    },
    [types.SAVE_MODEL_DRAFT]: function ({ commit }, para) {
      return api.model.saveModelDraft(para)
    },
    [types.CACHE_UPDATE_MODEL_EDIT]: function ({ commit }, para) {
      return api.model.updateModel(para)
    },
    [types.COLLECT_MODEL_STATS]: function ({ commit }, para) {
      return api.model.collectStats(para)
    },
    [types.DIAGNOSE]: function ({ commit }, diagnoseObj) {
      return api.model.diagnose(diagnoseObj.project, diagnoseObj.modelName)
    },
    [types.DIAGNOSELIST]: function ({ commit }, diagnoseObj) {
      return api.model.diagnoseList(diagnoseObj.project, diagnoseObj.pageOffset, diagnoseObj.pageSize).then((res) => {
        commit(types.CACHE_MODEL_DIANOSELIST, { data: res.data.data })
      })
    },
    [types.CHECK_MODELNAME]: function ({ commit }, modelName) {
      return api.model.checkModelName(modelName)
    },
    [types.GET_USED_COLS]: function ({ commit }, modelName) {
      return api.model.checkUsedCols(modelName)
    }
  },
  getters: {
    modelMixtureList: (state) => {
      for (var i = 0; i < state.modelsList.length; i++) {
        for (var s in state.modelsDianoseList) {
          if (state.modelsList[i].name === s) {
            state.modelsList[i].diagnose = state.modelsDianoseList[s]
          }
        }
      }
    }
  }
}

