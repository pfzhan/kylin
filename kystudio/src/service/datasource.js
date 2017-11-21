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
  getAclOfTable: (tableName, project, type) => {
    return Vue.resource(apiUrl + 'acl/table/' + project + '/' + tableName).get()
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
  getAclOfColumn: (tableName, project, type) => {
    return Vue.resource(apiUrl + 'acl/column/' + project + '/' + tableName).get()
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
  getAclOfRow: (tableName, project, type) => {
    return Vue.resource(apiUrl + 'acl/row/' + project + '/' + tableName).get()
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
  }

}
