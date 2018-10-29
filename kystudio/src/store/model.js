import api from './../service/api'
import * as types from './types'
import { transToGmtTime } from 'util/business'
export default {
  state: {
    modelsList: [],
    modelsDianoseList: [],
    modelsTotal: 0,
    modelEditCache: {},
    modelAccess: {},
    modelEndAccess: {},
    activeMenuName: '',
    modelSpeedEvents: 0,
    modelSpeedModelsCount: 0,
    reachThreshold: false
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
    },
    [types.CACHE_MODEL_ACCESS]: function (state, { access, id }) {
      state.modelAccess[id] = access
    },
    [types.CACHE_MODEL_END_ACCESS]: function (state, { access, id }) {
      state.modelEndAccess[id] = access
    },
    [types.CACHE_SPEED_INFO]: function (state, { reachThreshold, queryCount, modelCount }) {
      state.reachThreshold = reachThreshold
      state.modelSpeedEvents = queryCount
      state.modelSpeedModelsCount = modelCount
    }
  },
  actions: {
    [types.LOAD_MODEL_LIST]: function ({ commit }, para) {
      return new Promise((resolve, reject) => {
        api.model.getModelList(para).then((response) => {
          commit(types.SAVE_MODEL_LIST, { list: response.data.data.models, total: response.data.data.size })
          resolve(response)
        }, (res) => {
          commit(types.SAVE_MODEL_LIST, { list: [], total: 0 })
          reject(res)
        })
      })
    },
    [types.GET_SPEED_INFO]: function ({ commit }, projectName) {
      return new Promise((resolve, reject) => {
        api.model.getSpeedModelInfo(projectName).then((response) => {
          commit(types.CACHE_SPEED_INFO, {reachThreshold: response.data.data.reach_threshold, queryCount: response.data.data.size || [], modelCount: response.data.data.optimized_model_num})
          resolve(response)
        }, (response) => {
          commit(types.CACHE_SPEED_INFO, {reachThreshold: false, queryCount: 0, modelCount: 0})
          reject(response)
        })
      })
    },
    [types.APPLY_SPEED_INFO]: function ({ commit }, para) {
      return api.model.applySpeedModelInfo(para.project, para.size)
    },
    [types.IGNORE_SPEED_INFO]: function ({ commit }, projectName) {
      return api.model.ignoreSpeedModelInfo(projectName)
    },
    [types.PURGE_MODEL]: function ({ commit }, para) {
      return api.model.purgeModel(para.project, para.modelName)
    },
    [types.RENAME_MODEL]: function ({ commit }, para) {
      return api.model.renameModel(para)
    },
    [types.DISABLE_MODEL]: function ({ commit }, para) {
      return api.model.disableModel(para)
    },
    [types.ENABLE_MODEL]: function ({ commit }, para) {
      return api.model.enableModel(para)
    },
    [types.LOAD_ALL_MODEL]: function ({ commit }, para) {
      return api.model.getModelList(para)
    },
    [types.GET_TABLE_INDEX]: function ({ commit }, para) {
      return api.model.getTableIndex(para)
    },
    [types.EDIT_TABLE_INDEX]: function ({ commit }, para) {
      return api.model.editTableIndex(para)
    },
    [types.DELETE_TABLE_INDEX]: function ({ commit }, para) {
      return api.model.delTableIndex(para)
    },
    [types.ADD_TABLE_INDEX]: function ({ commit }, para) {
      return api.model.addTableIndex(para)
    },
    // old
    [types.SUGGEST_DIMENSION_MEASURE]: function ({ commit }, para) {
      return api.model.measureDimensionSuggestion(para)
    },
    [types.LOAD_MODEL_INFO]: function ({ commit }, para) {
      return api.model.getModelByModelName(para)
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
      return api.model.diagnoseList(diagnoseObj).then((res) => {
        commit(types.CACHE_MODEL_DIANOSELIST, { data: res.data.data })
      })
    },
    [types.CHECK_MODELNAME]: function ({ commit }, para) {
      return api.model.checkModelName(para)
    },
    [types.GET_USED_COLS]: function ({ commit }, modelName) {
      return api.model.checkUsedCols(modelName)
    },
    [types.GET_MODEL_PROGRESS]: function ({ commit }, para) {
      return api.model.modelProgress(para)
    },
    [types.MODEL_CHECKABLE]: function ({ commit }, para) {
      return api.model.modelCheckable(para)
    },
    [types.GET_MODEL_ACCESS]: function ({ commit }, id) {
      return api.model.getModelAccess(id).then((res) => {
        commit(types.CACHE_MODEL_ACCESS, {access: res.data.data, id: id})
        return res
      })
    },
    [types.GET_MODEL_END_ACCESS]: function ({ commit }, id) {
      return api.model.getModelEndAccess(id).then((res) => {
        commit(types.CACHE_MODEL_END_ACCESS, {access: res.data.data, id: id})
        return res
      })
    },
    [types.GET_COLUMN_SAMPLEDATA]: function ({ commit }, para) {
      return api.model.getColumnSampleData(para)
    },
    [types.VALID_PARTITION_COLUMN]: function ({ commit }, para) {
      return api.model.validModelPartitionColumnFormat(para)
    },
    [types.CHECK_COMPUTED_EXPRESSION]: function ({ commit }, para) {
      return api.model.checkComputedExpression(para)
    },
    [types.GET_COMPUTED_COLUMNS]: function ({ commit }, projectName) {
      return api.model.getComputedColumns(projectName)
    },
    [types.VERIFY_MODEL_SQL]: function ({ commit }, para) {
      return api.model.sqlValidate(para)
    },
    [types.AUTO_MODEL]: function ({ commit }, para) {
      return api.model.autoModel(para)
    },
    [types.VALID_AUTOMODEL_SQL]: function ({ commit }, para) {
      return api.model.validAutoModelSql(para)
    },
    [types.GET_AUTOMODEL_SQL]: function ({ commit }, para) {
      return api.model.getAutoModelSql(para)
    },
    [types.FETCH_AGGREGATES] ({ commit }, params) {
      return api.model.fetchAggregates(params.modelName, params.projectName)
    },
    [types.FETCH_SEGMENTS] ({ commit }, params) {
      return api.model.fetchSegments(params.modelName, params.projectName, String(params.startTime), String(params.endTime), params.pageOffset, params.pageSize)
    },
    [types.FETCH_CUBOID] ({ commit }, params) {
      return api.model.fetchCuboid(params.modelName, params.projectName, params.cuboidId)
    },
    [types.FETCH_CUBOIDS] ({ commit }, params) {
      return api.model.fetchCuboids(params.modelName, params.projectName)
    },
    [types.REFRESH_SEGMENTS] ({ commit }, params) {
      return api.model.refreshSegments(params.modelName, params.projectName, params.segmentIds)
    },
    [types.DELETE_SEGMENTS] ({ commit }, params) {
      return api.model.deleteSegments(params.modelName, params.projectName, params.segmentIds)
    },
    [types.MODEL_DATA_CHECK] ({ commit }, para) {
      return api.model.modelDataCheck(para)
    },
    [types.MODEL_BUILD] ({ commit }, para) {
      return api.model.buildModel(para)
    },
    [types.MODEL_PARTITION_SET] ({ commit }, para) {
      return api.model.setPartition(para)
    },
    [types.FETCH_AGGREGATE_GROUPS] ({ commit }, params) {
      return api.model.fetchAggregateGroups(params.projectName, params.modelName)
    },
    [types.UPDATE_AGGREGATE_GROUPS] ({ commit }, params) {
      return api.model.updateAggregateGroups(params.projectName, params.modelName, params.dimensions, params.aggregationGroups)
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
    },
    modelsPagerRenderData: (state) => {
      return {
        totalSize: state.modelsTotal,
        list: state.modelsList && state.modelsList.map((m) => {
          m.gmtTime = transToGmtTime(m.last_modified)
          return m
        }) || []
      }
    }
  }
}

