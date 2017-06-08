import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  loadDataSource: (project) => {
    return Vue.resource(apiUrl + 'tables?ext=true&project=' + project).get()
  },
  loadDataSourceExt: (tableName) => {
    return Vue.resource(apiUrl + 'table_ext/' + tableName).get()
  },
  loadBasicLiveDatabase: () => {
    return Vue.resource(apiUrl + 'tables/hive').get()
  },
  loadChildTablesOfDatabase: (database) => {
    return Vue.resource(apiUrl + 'tables/hive/' + database).get()
  },
  loadHiveInProject: (para) => {
    return Vue.resource(apiUrl + 'table_ext/' + para.tables + '/' + para.project).save(para.data)
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
    return Vue.resource(apiUrl + 'saved_queries' + para.projectName ? '/' + para.projectName : '').get(para.pageData)
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
  getTableJob: (tableName) => {
    return Vue.resource(apiUrl + 'table_ext/' + tableName + '/job').get()
  }
}
