import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  loadDataSource: (para) => {
    return Vue.resource(apiUrl + 'tables?ext=' + para.isExt + '&project=' + para.project).get()
  },
  loadDataSourceExt: (tableName, project) => {
    return Vue.resource(apiUrl + 'table_ext/' + project + '/' + tableName).get()
  },
  loadBasicLiveDatabase: () => {
    // return Vue.resource(apiUrl + 'tables/hive').get()
    return Vue.resource(apiUrl + 'tables/databases').get()
  },
  loadChildTablesOfDatabase: (database) => {
    return Vue.resource(apiUrl + 'tables/hive/' + database).get()
  },
  loadHiveInProject: (para) => {
    return Vue.resource(apiUrl + 'table_ext/' + para.project + '/load').save(para.data)
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
    return Vue.resource(apiUrl + 'saved_queries').save(para)
  },
  getSaveQueries: (para) => {
    return Vue.resource(apiUrl + 'saved_queries').get(para.pageData)
  },
  deleteQuery: (id) => {
    return Vue.resource(apiUrl + 'saved_queries/' + id).delete()
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
  getAclOfTable: (tableName, project) => {
    return Vue.resource(apiUrl + 'acl/table/' + project + '/' + tableName).get()
  },
  getAclBlackListOfTable: (tableName, project) => {
    return Vue.resource(apiUrl + 'acl/table/' + project + '/black/' + tableName).get()
  },
  saveAclSetOfTable: (tableName, project, userName) => {
    return Vue.resource(apiUrl + 'acl/table/' + project + '/' + tableName + '/' + userName).save()
  },
  cancelAclSetOfTable: (tableName, project, userName) => {
    return Vue.resource(apiUrl + 'acl/table/' + project + '/' + tableName + '/' + userName).delete()
  },
  getAclOfColumn: (tableName, project) => {
    return Vue.resource(apiUrl + 'acl/column/' + project + '/' + tableName).get()
  },
  getAclWhiteListOfColumn: (tableName, project) => {
    return Vue.resource(apiUrl + 'acl/column/white/' + project + '/' + tableName).get()
  },
  saveAclSetOfColumn: (tableName, project, userName, columnList) => {
    return Vue.resource(apiUrl + 'acl/column/' + project + '/' + tableName + '/' + userName).save(columnList)
  },
  updateAclSetOfColumn: (tableName, project, userName, columnList) => {
    return Vue.resource(apiUrl + 'acl/column/' + project + '/' + tableName + '/' + userName).update(columnList)
  },
  cancelAclSetOfColumn: (tableName, project, userName) => {
    return Vue.resource(apiUrl + 'acl/column/' + project + '/' + tableName + '/' + userName).delete()
  },
  // row
  getAclOfRow: (tableName, project) => {
    return Vue.resource(apiUrl + 'acl/row/' + project + '/' + tableName).get()
  },
  getAclWhiteListOfRow: (tableName, project) => {
    return Vue.resource(apiUrl + 'acl/row/available_user/' + project + '/' + tableName).get()
  },
  saveAclSetOfRow: (tableName, project, userName, conditions) => {
    return Vue.resource(apiUrl + 'acl/row/' + project + '/' + tableName + '/' + userName).save(conditions)
  },
  updateAclSetOfRow: (tableName, project, userName, conditions) => {
    return Vue.resource(apiUrl + 'acl/row/' + project + '/' + tableName + '/' + userName).update(conditions)
  },
  cancelAclSetOfRow: (tableName, project, userName) => {
    return Vue.resource(apiUrl + 'acl/row/' + project + '/' + tableName + '/' + userName).delete()
  },
  previewAclSetOfRowSql: (tableName, project, userName, conditions) => {
    return Vue.resource(apiUrl + 'acl/row/preview/' + project + '/' + tableName + '/' + userName).save(conditions)
  }

}
