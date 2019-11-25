import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  loadDataSource: (ext, project, database) => {
    return Vue.resource(apiUrl + 'tables').get({ext, project, database})
  },
  reloadDataSource: (data) => {
    return Vue.resource(apiUrl + 'tables/reload').save(data)
  },
  getReloadInfluence: (para) => {
    return Vue.resource(apiUrl + 'tables/prepare_reload').get(para)
  },
  loadDataSourceExt: (para) => {
    return Vue.resource(apiUrl + 'tables').get(para)
  },
  loadBasicLiveDatabaseTables: (project, datasourceType, database, table, page_offset, pageSize) => {
    return Vue.resource(apiUrl + 'tables/project_table_names').get({project, data_source_type: datasourceType, database, table, page_offset, page_size: pageSize})
  },
  loadBasicLiveDatabase: (project, datasourceType) => {
    // return Vue.resource(apiUrl + 'tables/hive').get()
    return Vue.resource(apiUrl + 'tables/databases').get({project, datasource_type: datasourceType})
  },
  loadChildTablesOfDatabase: (project, datasourceType, database, table, page_offset, pageSize) => {
    return Vue.resource(apiUrl + 'tables/names').get({project, data_source_type: datasourceType, database, table, page_offset, page_size: pageSize})
  },
  loadHiveInProject: (para) => {
    return Vue.resource(apiUrl + 'tables').save(para)
  },
  submitSampling: (para) => {
    return Vue.resource(apiUrl + 'tables/sampling_jobs').save(para)
  },
  hasSamplingJob: (para) => {
    return Vue.resource(apiUrl + 'tables/sampling_check_result').get(para)
  },
  unLoadHiveInProject: (data) => {
    return Vue.resource(apiUrl + 'table_ext/' + data.tables + '/' + data.project).delete()
  },
  loadBuildCompeleteTables: (project) => {
    return Vue.resource(apiUrl + 'tables_and_columns?project=' + project).get()
  },
  loadStatistics: (para) => {
    return Vue.resource(apiUrl + 'query/statistics/engine').get(para)
  },
  loadDashboardQueryInfo: (para) => {
    return Vue.resource(apiUrl + 'query/statistics').get(para)
  },
  loadQueryChartData: (para) => {
    return Vue.resource(apiUrl + 'query/statistics/count').get(para)
  },
  loadQueryDuraChartData: (para) => {
    return Vue.resource(apiUrl + 'query/statistics/duration').get(para)
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
    return Vue.resource(apiUrl + 'query/saved_queries/' + para.id + '?project=' + para.project).delete()
  },
  getRules: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules').get(para)
  },
  updateRules: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/rules').update(para)
  },
  getUserAndGroups: (para) => {
    return Vue.resource(apiUrl + 'user_group/users_and_groups').get(para)
  },
  getRulesImpact: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/accelerate_ratio').get(para)
  },
  getPreferrence: (para) => {
    return Vue.resource(apiUrl + 'projects/query_accelerate_threshold').get(para)
  },
  updatePreferrence: (para) => {
    return Vue.resource(apiUrl + 'projects/query_accelerate_threshold').update(para)
  },
  getHistoryList: (para) => {
    return Vue.resource(apiUrl + 'query/history_queries{?realization}{&query_status}').get(para)
  },
  loadOnlineQueryNodes: () => {
    return Vue.resource(apiUrl + 'query/servers').get()
  },
  getWaitingAcceSize: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/size').get(para)
  },
  getFavoriteList: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries{?status}').get(para)
  },
  importSqlFiles: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/sql_files?project=' + para.project).save(para.formData)
  },
  formatSql: (para) => {
    return Vue.resource(apiUrl + 'query/format').update(para)
  },
  validateWhite: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/sql_validation').update(para)
  },
  addToFavoriteList: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries').save(para)
  },
  removeFavSql: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries{?uuids}').delete(para)
  },
  loadBlackList: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/blacklist').get(para)
  },
  deleteBlack: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/blacklist/' + para.id).delete(para)
  },
  getCandidateList: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/candidates').get(para)
  },
  getEncoding: () => {
    return Vue.resource(apiUrl + 'cubes/valid_encodings').get()
  },
  getEncodingMatchs: () => {
    return Vue.resource(apiUrl + 'encodings/valid_encodings').get()
  },
  collectSampleData: (para) => {
    return Vue.resource(apiUrl + 'table_ext/' + para.tableName + '/sample_job?project=' + para.project).save(para.data)
  },
  getTableJob: (tableName, project) => {
    return Vue.resource(apiUrl + 'table_ext/' + tableName + '/job?project=' + project).get()
  },
  // acl
  getAclOfTable: (tableName, project, type, pager) => {
    return Vue.resource(apiUrl + 'acl/table/paged/' + tableName + '?project=' + project).get(pager)
  },
  getAclBlackListOfTable: (tableName, project, type, otherPara) => {
    return Vue.resource(apiUrl + 'acl/table/' + type + '/black/' + tableName + '?project=' + project).get(otherPara)
  },
  saveAclSetOfTable: (tableName, project, userName, type) => {
    return Vue.resource(apiUrl + 'acl/table/' + type + '/' + tableName + '/' + userName + '?project=' + project).save()
  },
  cancelAclSetOfTable: (tableName, project, userName, type) => {
    return Vue.resource(apiUrl + 'acl/table/' + type + '/' + tableName + '/' + userName + '?project=' + project).delete()
  },
  // column
  getAclOfColumn: (tableName, project, type, pager) => {
    return Vue.resource(apiUrl + 'acl/column/paged/' + tableName + '?project=' + project).get(pager)
  },
  getAclWhiteListOfColumn: (tableName, project, type, otherPara) => {
    return Vue.resource(apiUrl + 'acl/column/white/' + type + '/' + tableName + '?project=' + project).get(otherPara)
  },
  saveAclSetOfColumn: (tableName, project, userName, columnList, type) => {
    return Vue.resource(apiUrl + 'acl/column/' + type + '/' + tableName + '/' + userName + '?project=' + project).save(columnList)
  },
  updateAclSetOfColumn: (tableName, project, userName, columnList, type) => {
    return Vue.resource(apiUrl + 'acl/column/' + type + '/' + tableName + '/' + userName + '?project=' + project).update(columnList)
  },
  cancelAclSetOfColumn: (tableName, project, userName, type) => {
    return Vue.resource(apiUrl + 'acl/column/' + type + '/' + tableName + '/' + userName + '?project=' + project).delete()
  },
  // row
  getAclOfRow: (tableName, project, type, pager) => {
    return Vue.resource(apiUrl + 'acl/row/paged/' + tableName + '?project=' + project).get(pager)
  },
  getAclWhiteListOfRow: (tableName, project, type, otherPara) => {
    return Vue.resource(apiUrl + 'acl/row/white/' + type + '/' + tableName + '?project=' + project).get(otherPara)
  },
  saveAclSetOfRow: (tableName, project, userName, conditions, type) => {
    return Vue.resource(apiUrl + 'acl/row/' + type + '/' + tableName + '/' + userName + '?project=' + project).save(conditions)
  },
  updateAclSetOfRow: (tableName, project, userName, conditions, type) => {
    return Vue.resource(apiUrl + 'acl/row/' + type + '/' + tableName + '/' + userName + '?project=' + project).update(conditions)
  },
  cancelAclSetOfRow: (tableName, project, userName, type) => {
    return Vue.resource(apiUrl + 'acl/row/' + type + '/' + tableName + '/' + userName + '?project=' + project).delete()
  },
  previewAclSetOfRowSql: (tableName, project, userName, conditions) => {
    return Vue.resource(apiUrl + 'acl/row/preview/' + tableName + '?project=' + project).save(conditions)
  },
  saveTablePartition (body) {
    return Vue.resource(apiUrl + 'tables/partition_key').save(body)
  },
  saveDataRange (body) {
    return Vue.resource(apiUrl + 'tables/data_range').save(body)
  },
  fetchRelatedModels (project, table, model, page_offset, pageSize) {
    return Vue.resource(apiUrl + 'models').get({project, table, model, page_offset, page_size: pageSize, with_job_status: false})
  },
  fetchTables (project, database, table, page_offset, pageSize, isFuzzy, ext) {
    return Vue.resource(apiUrl + 'tables').get({project, database, table, page_offset, page_size: pageSize, is_fuzzy: isFuzzy, ext})
  },
  fetchDatabases (project, datasourceType) {
    return Vue.resource(apiUrl + 'tables/loaded_databases').get({project, datasource_type: datasourceType})
  },
  fetchDBandTables (project, page_offset, pageSize, table, datasourceType) {
    return Vue.resource(apiUrl + 'tables/project_tables').get({project, page_offset, page_size: pageSize, table, datasource_type: datasourceType})
  },
  reloadHiveDBAndTables (para) {
    return Vue.resource(apiUrl + 'tables/reload_hive_table_name').get(para)
  },
  updateTopTable (project, table, top) {
    return Vue.resource(apiUrl + 'tables/top').save({project, table, top})
  },
  deleteTable (project, database, table, cascade) {
    const para = { cascade }
    return Vue.resource(apiUrl + `tables/${database}/${table}?project=${project}`).delete(para)
  },
  prepareUnload (para) {
    return Vue.resource(apiUrl + `tables/${para.databaseName}/${para.tableName}/prepare_unload?project=${para.projectName}`).get()
  },
  fetchChangeTypeInfo (project, table, action) {
    return Vue.resource(apiUrl + `models/affected_models`).get({ project, table, action })
  },
  fetchRangeFreshInfo (project, table, start, end) {
    return Vue.resource(apiUrl + `tables/affected_data_range`).get({ project, table, start, end })
  },
  freshRangeData (project, table, refreshStart, refreshEnd, affectedStart, affectedEnd) {
    return Vue.resource(apiUrl + `tables/data_range`).update({ project, table, refresh_start: refreshStart, refresh_end: refreshEnd, affected_start: affectedStart, affected_end: affectedEnd })
  },
  fetchMergeConfig (project, model, table) {
    return Vue.resource(apiUrl + `tables/auto_merge_config`).get({ project, model, table })
  },
  updateMergeConfig (project, model, table, autoMergeEnabled, autoMergeTimeRanges, volatileRangeEnabled, volatileRangeNumber, volatileRangeType) {
    return Vue.resource(apiUrl + `tables/auto_merge_config`).update({ project, model, table, auto_merge_enabled: autoMergeEnabled, auto_merge_time_ranges: autoMergeTimeRanges, volatile_range_number: volatileRangeNumber, volatile_range_type: volatileRangeType, volatile_range_enabled: volatileRangeEnabled })
  },
  fetchPushdownConfig (project, table) {
    return Vue.resource(apiUrl + `tables/pushdown_mode`).get({ project, table })
  },
  updatePushdownConfig (project, table, pushdownRangeLimited) {
    return Vue.resource(apiUrl + `tables/pushdown_mode`).update({ project, table, pushdown_range_limited: pushdownRangeLimited })
  },
  discardTableModel (project, modelId, status) {
    return Vue.resource(apiUrl + `models/${modelId}/management_type`).update({ project, model_id: modelId })
  },
  fetchNewestTableRange (project, table) {
    return Vue.resource(apiUrl + `tables/data_range/latest_data`).get({ project, table })
  },
  fetchBatchLoadTables (project) {
    return Vue.resource(apiUrl + `tables/batch_load`).get({ project })
  },
  saveTablesBatchLoad (body) {
    return Vue.resource(apiUrl + `tables/batch_load`).save(body)
  },
  saveSourceConfig (body) {
    return new Promise((resolve) => setTimeout(() => resolve(), 1000))
  },
  // csv 数据源
  // 联通性测试
  verifyCsvConnection (para) {
    return Vue.resource(apiUrl + `source/verify`).save(para)
  },
  // 样例数据
  getCsvSampleData (para) {
    return Vue.resource(apiUrl + `source/csv/samples`).save(para)
  },
  // 保存csv数据源（包含表）
  saveCsvInfo (type, data) {
    return Vue.resource(apiUrl + `source/csv/save?mode=` + type).save(data)
  },
  loadCsvSchema (data) {
    return Vue.resource(apiUrl + `source/csv/schema`).save(data)
  },
  verifyCSVSql (data) {
    return Vue.resource(apiUrl + `source/validate`).save(data)
  },
  fetchPartitionFormat (data) {
    return Vue.resource(apiUrl + 'tables/partition_column_format').get(data)
  }
}
