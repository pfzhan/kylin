import api from './../service/api'
import * as types from './types'
export default {
  state: {
    dataSource: {},
    encodings: null,
    encodingMatchs: null,
    encodingCache: {},
    currentShowTableData: null
  },
  getters: {
  },
  mutations: {
    [types.CACHE_DATASOURCE]: function (state, { data, project, isReset = true }) {
      state.dataSource[project] = !isReset ? [...state.dataSource[project], ...data] : data
    },
    [types.CACHE_ENCODINGS]: function (state, { data, project }) {
      state.encodings = data
    },
    [types.CACHE_ENCODINGMATCHS]: function (state, { data, project }) {
      state.encodingMatchs = data
    },
    [types.SET_CURRENT_TABLE] (state, { tableData }) {
      state.currentShowTableData = tableData
    }
  },
  actions: {
    [types.LOAD_DATASOURCE]: function ({ commit }, para) {
      return api.datasource.loadDataSource(para.isExt, para.project, para.datasource)
        .then((response) => {
          commit(types.CACHE_DATASOURCE, { data: response.data.data, project: para.project })
          return response
        })
    },
    [types.IMPORT_DATABASE]: function ({commit}, para) {
      return api.datasource.importDatabases(para.projectName, para.sourceType, para.databaseNames)
    },
    [types.LOAD_DATASOURCE_EXT]: function ({commit}, para) {
      return api.datasource.loadDataSourceExt({ext: true, project: para.project, table: para.tableName})
    },
    [types.LOAD_HIVEBASIC_DATABASE]: function ({commit}, para) {
      return api.datasource.loadBasicLiveDatabase(para.projectName, para.sourceType)
    },
    [types.LOAD_HIVE_TABLES]: function ({commit}, para) {
      return api.datasource.loadChildTablesOfDatabase(para.projectName, para.sourceType, para.databaseName, para.tableName, para.pageOffset, para.pageSize)
    },
    [types.LOAD_HIVE_IN_PROJECT]: function ({commit}, para) {
      return api.datasource.loadHiveInProject(para.projectName, para.sourceType, para.tableNames)
    },
    [types.UN_LOAD_HIVE_IN_PROJECT]: function ({commit}, data) {
      return api.datasource.unLoadHiveInProject(data)
    },
    [types.LOAD_BUILD_COMPLETE_TABLES]: function ({commit}, project) {
      return api.datasource.loadBuildCompeleteTables(project)
    },
    [types.LOAD_STATISTICS]: function ({commit}, para) {
      return api.datasource.loadStatistics(para)
    },
    [types.QUERY_BUILD_TABLES]: function ({commit}, para) {
      return api.datasource.query(para)
    },
    [types.SAVE_QUERY]: function ({commit}, para) {
      return api.datasource.saveQuery(para)
    },
    [types.GET_SAVE_QUERIES]: function ({commit}, para) {
      return api.datasource.getSaveQueries(para)
    },
    [types.DELETE_QUERY]: function ({commit}, para) {
      return api.datasource.deleteQuery(para)
    },
    [types.DELETE_FAV]: function ({commit}, para) {
      return api.datasource.deleteFav(para)
    },
    [types.MARK_FAV]: function ({commit}, para) {
      return api.datasource.markFav(para)
    },
    [types.GET_ALL_RULES]: function ({commit}, para) {
      return api.datasource.getAllrules(para)
    },
    [types.UPDATE_RULE]: function ({commit}, para) {
      return api.datasource.updateRule(para)
    },
    [types.SAVE_RULE]: function ({commit}, para) {
      return api.datasource.saveRule(para)
    },
    [types.DELETE_RULE]: function ({commit}, para) {
      return api.datasource.deleteRule(para)
    },
    [types.ENABLE_RULE]: function ({commit}, para) {
      return api.datasource.enableRule(para)
    },
    [types.APPLY_RULE]: function ({commit}, para) {
      return api.datasource.applyRule(para)
    },
    [types.AUTOMATIC_RULE]: function ({commit}, para) {
      return api.datasource.autoMaticRule(para)
    },
    [types.GET_AUTO_MATIC_STATUS]: function ({commit}, para) {
      return api.datasource.getAutoMaticStatus(para)
    },
    [types.GET_HISTORY_LIST]: function ({commit}, para) {
      return api.datasource.getHistoryList(para)
    },
    [types.GET_FAVORITE_LIST]: function ({commit}, para) {
      return api.datasource.getFavoriteList(para)
    },
    [types.GET_CANDIDATE_LIST]: function ({commit}, para) {
      return api.datasource.getCandidateList(para)
    },
    [types.GET_ENCODINGS]: function ({commit}) {
      return api.datasource.getEncoding().then((response) => {
        commit(types.CACHE_ENCODINGS, {data: response.data.data})
        api.datasource.getEncodingMatchs().then((response) => {
          commit(types.CACHE_ENCODINGMATCHS, {data: response.data.data})
        })
      })
    },
    [types.COLLECT_SAMPLE_DATA]: function ({commit}, para) {
      return api.datasource.collectSampleData(para)
    },
    [types.GET_TABLE_JOB]: function ({commit}, para) {
      return api.datasource.getTableJob(para.tableName, para.project)
    },
    // acl table
    [types.GET_ACL_SET_TABLE]: function ({commit}, para) {
      return api.datasource.getAclOfTable(para.tableName, para.project, para.type, para.pager)
    },
    [types.SAVE_ACL_SET_TABLE]: function ({commit}, para) {
      return api.datasource.saveAclSetOfTable(para.tableName, para.project, para.userName, para.type)
    },
    [types.DEL_ACL_SET_TABLE]: function ({commit}, para) {
      return api.datasource.cancelAclSetOfTable(para.tableName, para.project, para.userName, para.type)
    },
    [types.GET_ACL_BLACKLIST_TABLE]: function ({commit}, para) {
      return api.datasource.getAclBlackListOfTable(para.tableName, para.project, para.type, para.otherPara)
    },
    // acl column
    [types.GET_ACL_SET_COLUMN]: function ({commit}, para) {
      return api.datasource.getAclOfColumn(para.tableName, para.project, para.type, para.pager)
    },
    [types.SAVE_ACL_SET_COLUMN]: function ({commit}, para) {
      return api.datasource.saveAclSetOfColumn(para.tableName, para.project, para.userName, para.columns, para.type)
    },
    [types.UPDATE_ACL_SET_COLUMN]: function ({commit}, para) {
      return api.datasource.updateAclSetOfColumn(para.tableName, para.project, para.userName, para.columns, para.type)
    },
    [types.DEL_ACL_SET_COLUMN]: function ({commit}, para) {
      return api.datasource.cancelAclSetOfColumn(para.tableName, para.project, para.userName, para.type)
    },
    [types.GET_ACL_WHITELIST_COLUMN]: function ({commit}, para) {
      return api.datasource.getAclWhiteListOfColumn(para.tableName, para.project, para.type, para.otherPara)
    },
    // acl row
    [types.GET_ACL_SET_ROW]: function ({commit}, para) {
      return api.datasource.getAclOfRow(para.tableName, para.project, para.type, para.pager)
    },
    [types.SAVE_ACL_SET_ROW]: function ({commit}, para) {
      return api.datasource.saveAclSetOfRow(para.tableName, para.project, para.userName, para.conditions, para.type)
    },
    [types.UPDATE_ACL_SET_ROW]: function ({commit}, para) {
      return api.datasource.updateAclSetOfRow(para.tableName, para.project, para.userName, para.conditions, para.type)
    },
    [types.DEL_ACL_SET_ROW]: function ({commit}, para) {
      return api.datasource.cancelAclSetOfRow(para.tableName, para.project, para.userName, para.type)
    },
    [types.GET_ACL_WHITELIST_ROW]: function ({commit}, para) {
      return api.datasource.getAclWhiteListOfRow(para.tableName, para.project, para.type, para.otherPara)
    },
    [types.PREVIEW_ACL_SET_ROW_SQL]: function ({commit}, para) {
      return api.datasource.previewAclSetOfRowSql(para.tableName, para.project, para.userName, para.conditions)
    },
    [types.SAVE_FACT_TABLE]: function ({commit}, para) {
      return api.datasource.saveFactTable(para.projectName, para.tableFullName, para.isIncremental, para.column)
    },
    [types.SAVE_DATA_RANGE]: function ({commit}, para) {
      return api.datasource.saveDataRange(para.projectName, para.tableFullName, String(para.startTime), String(para.endTime))
    },
    [types.FETCH_RELATED_MODELS]: function ({commit}, para) {
      return api.datasource.fetchRelatedModels(para.projectName, para.tableFullName, para.pageOffset, para.pageSize)
    },
    [types.FETCH_DATABASES]: function ({commit}, para) {
      return api.datasource.fetchDatabases(para.projectName, para.sourceType)
    },
    [types.FETCH_TABLES]: function ({commit}, para) {
      return api.datasource.fetchTables(para.projectName, para.databaseName, para.tableName, para.pageOffset, para.pageSize, para.isFuzzy, para.isExt)
        .then((response) => {
          commit(types.CACHE_DATASOURCE, { data: response.data.data, project: para.projectName, isReset: para.pageOffset === 0 })
          return response
        })
    },
    [types.UPDATE_TOP_TABLE]: function ({commit}, para) {
      return api.datasource.updateTopTable(para.projectName, para.tableFullName, para.isTopSet)
    },
    [types.DELETE_TABLE]: function ({commit}, para) {
      return api.datasource.deleteTable(para.projectName, para.databaseName, para.tableName)
    },
    [types.FETCH_RANGE_FRESH_INFO]: function ({commit}, para) {
      return api.datasource.fetchRangeFreshInfo(para.projectName, para.tableFullName, String(para.startTime), String(para.endTime))
    },
    [types.FRESH_RANGE_DATA]: function ({commit}, para) {
      return api.datasource.freshRangeData(para.projectName, para.tableFullName, String(para.startTime), String(para.endTime))
    },
    [types.FETCH_MERGE_CONFIG]: function ({commit}, para) {
      return api.datasource.fetchMergeConfig(para.projectName, para.modelName, para.tableFullName)
    },
    [types.UPDATE_MERGE_CONFIG]: function ({commit}, para) {
      return api.datasource.updateMergeConfig(para.projectName, para.modelName, para.tableFullName, para.isAutoMerge, para.autoMergeConfigs, para.isVolatile, para.volatileConfig.value, para.volatileConfig.type)
    },
    [types.FETCH_PUSHDOWN_CONFIG]: function ({commit}, para) {
      return api.datasource.fetchPushdownConfig(para.projectName, para.tableFullName)
    },
    [types.UPDATE_PUSHDOWN_CONFIG]: function ({commit}, para) {
      return api.datasource.updatePushdownConfig(para.projectName, para.tableFullName, para.isPushdownSync)
    }
  }
}
