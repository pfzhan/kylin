import api from './../service/api'
import * as types from './types'
export default {
  state: {
    dataSource: {},
    encodings: null,
    encodingMatchs: null,
    encodingCache: {},
    savedQueries: [],
    savedQueriesSize: 0,
    currentShowTableData: null
  },
  getters: {
  },
  mutations: {
    [types.CACHE_DATASOURCE]: function (state, { data, project }) {
      state.dataSource[project] = data
    },
    [types.CACHE_ENCODINGS]: function (state, { data, project }) {
      state.encodings = data
    },
    [types.CACHE_ENCODINGMATCHS]: function (state, { data, project }) {
      state.encodingMatchs = data
    },
    [types.SAVED_QUERIES]: function (state, { data, size }) {
      state.savedQueries = data
      state.savedQueriesSize = size
    }
  },
  actions: {
    [types.LOAD_DATASOURCE]: function ({ commit }, para) {
      return api.datasource.loadDataSource(para).then((response) => {
        commit(types.CACHE_DATASOURCE, { data: response.data.data, project: para.project })
        return response
      })
    },
    [types.LOAD_DATASOURCE_EXT]: function ({commit}, para) {
      return api.datasource.loadDataSourceExt(para.tableName, para.project)
    },
    [types.LOAD_HIVEBASIC_DATABASE]: function ({commit}) {
      return api.datasource.loadBasicLiveDatabase()
    },
    [types.LOAD_HIVE_TABLES]: function ({commit}, database) {
      return api.datasource.loadChildTablesOfDatabase(database)
    },
    [types.LOAD_HIVE_IN_PROJECT]: function ({commit}, data) {
      return api.datasource.loadHiveInProject(data)
    },
    [types.UN_LOAD_HIVE_IN_PROJECT]: function ({commit}, data) {
      return api.datasource.unLoadHiveInProject(data)
    },
    [types.LOAD_BUILD_COMPLETE_TABLES]: function ({commit}, project) {
      return api.datasource.loadBuildCompeleteTables(project)
    },
    [types.QUERY_BUILD_TABLES]: function ({commit}, para) {
      return api.datasource.query(para)
    },
    [types.SAVE_QUERY]: function ({commit}, para) {
      return api.datasource.saveQuery(para)
    },
    [types.GET_SAVE_QUERIES]: function ({commit}, para) {
      return api.datasource.getSaveQueries(para).then((res) => {
        commit(types.SAVED_QUERIES, {
          data: res.data.data.queries,
          size: res.data.data.size
        })
      })
    },
    [types.DELETE_QUERY]: function ({commit}, queryId) {
      return api.datasource.deleteQuery(queryId)
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
      return api.datasource.getAclOfTable(para.tableName, para.project)
    },
    [types.SAVE_ACL_SET_TABLE]: function ({commit}, para) {
      return api.datasource.saveAclSetOfTable(para.tableName, para.project, para.userName)
    },
    [types.DEL_ACL_SET_TABLE]: function ({commit}, para) {
      return api.datasource.cancelAclSetOfTable(para.tableName, para.project, para.userName)
    },
    [types.GET_ACL_BLACKLIST_TABLE]: function ({commit}, para) {
      return api.datasource.getAclBlackListOfTable(para.tableName, para.project)
    },
    // acl column
    [types.GET_ACL_SET_COLUMN]: function ({commit}, para) {
      return api.datasource.getAclOfColumn(para.tableName, para.project)
    },
    [types.SAVE_ACL_SET_COLUMN]: function ({commit}, para) {
      return api.datasource.saveAclSetOfColumn(para.tableName, para.project, para.userName, para.columns)
    },
    [types.UPDATE_ACL_SET_COLUMN]: function ({commit}, para) {
      return api.datasource.updateAclSetOfColumn(para.tableName, para.project, para.userName, para.columns)
    },
    [types.DEL_ACL_SET_COLUMN]: function ({commit}, para) {
      return api.datasource.cancelAclSetOfColumn(para.tableName, para.project, para.userName)
    },
    [types.GET_ACL_WHITELIST_COLUMN]: function ({commit}, para) {
      return api.datasource.getAclWhiteListOfColumn(para.tableName, para.project)
    },
    // acl row
    [types.GET_ACL_SET_ROW]: function ({commit}, para) {
      return api.datasource.getAclOfRow(para.tableName, para.project)
    },
    [types.SAVE_ACL_SET_ROW]: function ({commit}, para) {
      return api.datasource.saveAclSetOfRow(para.tableName, para.project, para.userName, para.conditions)
    },
    [types.UPDATE_ACL_SET_ROW]: function ({commit}, para) {
      return api.datasource.updateAclSetOfRow(para.tableName, para.project, para.userName, para.conditions)
    },
    [types.DEL_ACL_SET_ROW]: function ({commit}, para) {
      return api.datasource.cancelAclSetOfRow(para.tableName, para.project, para.userName)
    },
    [types.GET_ACL_WHITELIST_ROW]: function ({commit}, para) {
      return api.datasource.getAclWhiteListOfRow(para.tableName, para.project)
    },
    [types.PREVIEW_ACL_SET_ROW_SQL]: function ({commit}, para) {
      return api.datasource.previewAclSetOfRowSql(para.tableName, para.project, para.userName, para.conditions)
    }
  }
}
