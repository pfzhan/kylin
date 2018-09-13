import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  loadDataSource: (para) => {
    return Vue.resource(apiUrl + 'tables?ext=' + para.isExt + '&project=' + para.project).get()
  },
  loadDataSourceExt: (para) => {
    return Vue.resource(apiUrl + 'tables').get(para)
  },
  loadBasicLiveDatabase: (project, datasourceType) => {
    // return Vue.resource(apiUrl + 'tables/hive').get()
    return Vue.resource(apiUrl + 'tables/databases').get({project, datasourceType})
  },
  loadChildTablesOfDatabase: (project, datasourceType, database) => {
    return Vue.resource(apiUrl + 'tables/names').get({project, datasourceType, database})
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
  deleteQuery: (id) => {
    return Vue.resource(apiUrl + 'saved_queries/' + id).delete()
  },
  deleteFav: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries').delete(para)
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
    return Vue.resource(apiUrl + 'query/favorite_queries/rules/' + para.project + '/' + para.uuid).update(para)
  },
  applyRule: (para) => {
    return Vue.resource(apiUrl + 'query/favorites_queries/rules/apply/' + para.project).update()
  },
  autoMaticRule: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules/automatic/' + para.project).update()
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
  saveDateRange (project, table, startTime, endTime) {
    return Vue.resource(apiUrl + 'tables/date_range').save({project, table, startTime, endTime})
  },
  fetchRelatedModels (project, table) {
    return Vue.resource(apiUrl + 'models').get({project, table})
  }
}
