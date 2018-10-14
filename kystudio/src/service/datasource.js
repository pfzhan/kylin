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
  loadHiveInProject: (project, datasourceType, tables) => {
    return Vue.resource(apiUrl + 'tables').save({project, datasourceType, tables})
  },
  unLoadHiveInProject: (data) => {
    return Vue.resource(apiUrl + 'table_ext/' + data.tables + '/' + data.project).delete()
  },
  loadBuildCompeleteTables: (project) => {
    return Vue.resource(apiUrl + 'tables_and_columns?project=' + project).get()
  },
  query: (para) => {
    return Vue.resource(apiUrl + 'query').save(para)
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
  getAllrules: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules').get(para)
  },
  updateRule: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules').update(para)
  },
  saveRule: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules').save(para)
  },
  deleteRule: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules/' + para.project + '/' + para.uuid).delete()
  },
  enableRule: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules/enable/' + para.project + '/' + para.uuid).update()
  },
  applyRule: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules/apply/' + para.project).update()
  },
  autoMaticRule: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules/automatic/' + para.project).update()
  },
  getAutoMaticStatus: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules/automatic').get(para)
  },
  getHistoryList: (para) => {
    return Vue.resource(apiUrl + 'query/history_queries').get(para)
  },
  getFavoriteList: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries').get(para)
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
  saveFactTable (project, table, fact, column) {
    return Vue.resource(apiUrl + 'tables/fact').save({project, table, fact, column})
  },
  saveDateRange (project, table, start, end) {
    return Vue.resource(apiUrl + 'tables/date_range').save({project, table, start, end})
  },
  fetchRelatedModels (project, table) {
    return Vue.resource(apiUrl + 'models').get({project, table})
  },
  fetchTables (project, database, table, pageOffset, pageSize, isFuzzy, ext) {
    return Vue.resource(apiUrl + 'tables').get({project, database, table, pageOffset, pageSize, isFuzzy, ext})
  },
  fetchDatabases (project, datasourceType) {
    return Vue.resource(apiUrl + 'tables/databases').get({project, datasourceType})
  },
  importDatabases (project, datasourceType, databases) {
    return Vue.resource(apiUrl + 'tables/databases').save({project, datasourceType, databases})
  },
  updateTopTable (project, table, top) {
    return Vue.resource(apiUrl + 'tables/top').save({project, table, top})
  },
  deleteTable (project, database, table) {
    return Vue.resource(apiUrl + `tables/${project}/${database}/${table}`).delete()
  }
}
