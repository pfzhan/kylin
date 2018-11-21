import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  loadDataSource: (ext, project, database) => {
    return Vue.resource(apiUrl + 'tables').get({ext, project, database})
  },
  loadDataSourceExt: (para) => {
    return Vue.resource(apiUrl + 'tables').get(para)
  },
  loadBasicLiveDatabase: (project, datasourceType) => {
    // return Vue.resource(apiUrl + 'tables/hive').get()
    return Vue.resource(apiUrl + 'tables/databases').get({project, datasourceType})
  },
  loadChildTablesOfDatabase: (project, datasourceType, database, table, pageOffset, pageSize) => {
    return Vue.resource(apiUrl + 'tables/names').get({project, datasourceType, database, table, pageOffset, pageSize})
  },
  loadHiveInProject: (project, datasourceType, tables, databases) => {
    return Vue.resource(apiUrl + 'tables').save({project, datasourceType, tables, databases})
  },
  unLoadHiveInProject: (data) => {
    return Vue.resource(apiUrl + 'table_ext/' + data.tables + '/' + data.project).delete()
  },
  loadBuildCompeleteTables: (project) => {
    return Vue.resource(apiUrl + 'tables_and_columns?project=' + project).get()
  },
  loadStatistics: (para) => {
    return Vue.resource(apiUrl + 'query/statistics').get(para)
  },
  query: (para) => {
    const vm = window.kapVm
    return vm.$http.post(apiUrl + 'query', para, {headers: {'X-Progress-Invisiable': 'true'}})
  },
  saveQuery: (para) => {
    return Vue.resource(apiUrl + 'query/saved_queries').save(para)
  },
  getSaveQueries: (para) => {
    return Vue.resource(apiUrl + 'query/saved_queries').get(para)
  },
  deleteQuery: (para) => {
    return Vue.resource(apiUrl + 'query/saved_queries/' + para.project + '/' + para.id).delete()
  },
  deleteFav: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/unfavorite').save(para)
  },
  markFav: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries').save(para)
  },
  getFrequency: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules/frequency').get(para)
  },
  getSubmitter: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules/submitter').get(para)
  },
  getDuration: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules/duration').get(para)
  },
  getRulesImpact: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules/impact').get(para)
  },
  getPreferrence: (para) => {
    return Vue.resource(apiUrl + 'projects/query_accelerate_threshold').get(para)
  },
  updateFrequency: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules/frequency').update(para)
  },
  updateSubmitter: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules/submitter').update(para)
  },
  updateDuration: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules/duration').update(para)
  },
  updatePreferrence: (para) => {
    return Vue.resource(apiUrl + 'projects/query_accelerate_threshold').update(para)
  },
  getHistoryList: (para) => {
    return Vue.resource(apiUrl + 'query/history_queries').get(para)
  },
  getFavoriteList: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries').get(para)
  },
  loadWhiteList: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/whitelist').get(para)
  },
  saveWhite: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/whitelist').update(para)
  },
  deleteWhite: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/whitelist').delete(para)
  },
  loadBlackList: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/blacklist').get(para)
  },
  addBlack: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/blacklist').save(para)
  },
  deleteBlack: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/blacklist').delete(para)
  },
  getCandidateList: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/candidates').get(para)
  },
  getEncoding: () => {
    return Vue.resource(apiUrl + 'cubes/validEncodings').get()
  },
  getEncodingMatchs: () => {
    return Vue.resource(apiUrl + 'encodings/valid_encodings').get()
  },
  collectSampleData: (para) => {
    return Vue.resource(apiUrl + 'table_ext/' + para.project + '/' + para.tableName + '/sample_job').save(para.data)
  },
  getTableJob: (tableName, project) => {
    return Vue.resource(apiUrl + 'table_ext/' + project + '/' + tableName + '/job').get()
  },
  // acl
  getAclOfTable: (tableName, project, type, pager) => {
    return Vue.resource(apiUrl + 'acl/table/paged/' + project + '/' + tableName).get(pager)
  },
  getAclBlackListOfTable: (tableName, project, type, otherPara) => {
    return Vue.resource(apiUrl + 'acl/table/' + project + '/' + type + '/black/' + tableName).get(otherPara)
  },
  saveAclSetOfTable: (tableName, project, userName, type) => {
    return Vue.resource(apiUrl + 'acl/table/' + project + '/' + type + '/' + tableName + '/' + userName).save()
  },
  cancelAclSetOfTable: (tableName, project, userName, type) => {
    return Vue.resource(apiUrl + 'acl/table/' + project + '/' + type + '/' + tableName + '/' + userName).delete()
  },
  // column
  getAclOfColumn: (tableName, project, type, pager) => {
    return Vue.resource(apiUrl + 'acl/column/paged/' + project + '/' + tableName).get(pager)
  },
  getAclWhiteListOfColumn: (tableName, project, type, otherPara) => {
    return Vue.resource(apiUrl + 'acl/column/white/' + project + '/' + type + '/' + tableName).get(otherPara)
  },
  saveAclSetOfColumn: (tableName, project, userName, columnList, type) => {
    return Vue.resource(apiUrl + 'acl/column/' + project + '/' + type + '/' + tableName + '/' + userName).save(columnList)
  },
  updateAclSetOfColumn: (tableName, project, userName, columnList, type) => {
    return Vue.resource(apiUrl + 'acl/column/' + project + '/' + type + '/' + tableName + '/' + userName).update(columnList)
  },
  cancelAclSetOfColumn: (tableName, project, userName, type) => {
    return Vue.resource(apiUrl + 'acl/column/' + project + '/' + type + '/' + tableName + '/' + userName).delete()
  },
  // row
  getAclOfRow: (tableName, project, type, pager) => {
    return Vue.resource(apiUrl + 'acl/row/paged/' + project + '/' + tableName).get(pager)
  },
  getAclWhiteListOfRow: (tableName, project, type, otherPara) => {
    return Vue.resource(apiUrl + 'acl/row/white/' + project + '/' + type + '/' + tableName).get(otherPara)
  },
  saveAclSetOfRow: (tableName, project, userName, conditions, type) => {
    return Vue.resource(apiUrl + 'acl/row/' + project + '/' + type + '/' + tableName + '/' + userName).save(conditions)
  },
  updateAclSetOfRow: (tableName, project, userName, conditions, type) => {
    return Vue.resource(apiUrl + 'acl/row/' + project + '/' + type + '/' + tableName + '/' + userName).update(conditions)
  },
  cancelAclSetOfRow: (tableName, project, userName, type) => {
    return Vue.resource(apiUrl + 'acl/row/' + project + '/' + type + '/' + tableName + '/' + userName).delete()
  },
  previewAclSetOfRowSql: (tableName, project, userName, conditions) => {
    return Vue.resource(apiUrl + 'acl/row/preview/' + project + '/' + tableName).save(conditions)
  },
  saveFactTable (project, table, fact, column, format) {
    return Vue.resource(apiUrl + 'tables/fact').save({project, table, fact, column, partition_date_format: format})
  },
  saveDataRange (project, table, start, end) {
    return Vue.resource(apiUrl + 'tables/data_range').save({project, table, start, end})
  },
  fetchRelatedModels (project, table, model, pageOffset, pageSize) {
    return Vue.resource(apiUrl + 'models').get({project, table, model, pageOffset, pageSize})
  },
  fetchTables (project, database, table, pageOffset, pageSize, isFuzzy, ext) {
    return Vue.resource(apiUrl + 'tables').get({project, database, table, pageOffset, pageSize, isFuzzy, ext})
  },
  fetchDatabases (project, datasourceType) {
    return Vue.resource(apiUrl + 'tables/loaded_databases').get({project, datasourceType})
  },
  updateTopTable (project, table, top) {
    return Vue.resource(apiUrl + 'tables/top').save({project, table, top})
  },
  deleteTable (project, database, table) {
    return Vue.resource(apiUrl + `tables/${project}/${database}/${table}`).delete()
  },
  fetchChangeTypeInfo (project, table, fact) {
    return Vue.resource(apiUrl + `models/affected_models`).get({ project, table, fact })
  },
  fetchRangeFreshInfo (project, table, start, end) {
    return Vue.resource(apiUrl + `tables/affected_data_range`).get({ project, table, start, end })
  },
  freshRangeData (project, table, refreshStart, refreshEnd, affectedStart, affectedEnd) {
    return Vue.resource(apiUrl + `tables/data_range`).update({ project, table, refreshStart, refreshEnd, affectedStart, affectedEnd })
  },
  fetchMergeConfig (project, model, table) {
    return Vue.resource(apiUrl + `tables/auto_merge_config`).get({ project, model, table })
  },
  updateMergeConfig (project, model, table, autoMergeEnabled, autoMergeTimeRanges, volatileRangeEnabled, volatileRangeNumber, volatileRangeType) {
    return Vue.resource(apiUrl + `tables/auto_merge_config`).update({ project, model, table, autoMergeEnabled, autoMergeTimeRanges, volatileRangeNumber, volatileRangeType, volatileRangeEnabled })
  },
  fetchPushdownConfig (project, table) {
    return Vue.resource(apiUrl + `tables/pushdown_mode`).get({ project, table })
  },
  updatePushdownConfig (project, table, pushdownRangeLimited) {
    return Vue.resource(apiUrl + `tables/pushdown_mode`).update({ project, table, pushdownRangeLimited })
  },
  discardTableModel (project, modelName, status) {
    return Vue.resource(apiUrl + `models/management_type`).update({ project, modelName })
  }
}
