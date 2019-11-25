import { sourceTypes } from '../../../config'
import { collectObject } from '../../../util'
export const editTypes = {
  SELECT_SOURCE: 'selectSource',
  CONFIG_SOURCE: 'configSource',
  VIEW_SOURCE: 'viewSource',
  CONFIG_CSV_SETTING: 'configCsvSetting',
  CONFIG_CSV_STRUCTURE: 'configCsvStructure',
  CONFIG_CSV_SQL: 'configCsvSql',
  ...sourceTypes
}
// export const sourceSettingSteps = {
//   hive: [],
//   csv: {
//     step: 'SELECT_SOURCE',
//     children:
//   }
// }
export const titleMaps = {
  [editTypes.SELECT_SOURCE]: 'selectSource',
  [editTypes.CONFIG_SOURCE]: 'configSource',
  [editTypes.VIEW_SOURCE]: 'viewSource',
  // csv 数据源相关
  [editTypes.CSV]: 'configCsvConn',
  [editTypes.CONFIG_CSV_SETTING]: 'configCsvSetting',
  [editTypes.CONFIG_CSV_STRUCTURE]: 'configCsvSturcture',
  [editTypes.CONFIG_CSV_SQL]: 'configCsvSql',
  [editTypes.HIVE]: 'loadhiveTables',
  [editTypes.RDBMS]: 'loadTables',
  [editTypes.RDBMS2]: 'loadTables',
  [editTypes.KAFKA]: 'loadKafkaTopic'
}

export const cancelMaps = {
  [editTypes.SELECT_SOURCE]: 'kylinLang.common.cancel',
  [editTypes.CONFIG_SOURCE]: 'kylinLang.common.prev',
  [editTypes.CSV]: 'kylinLang.common.prev',
  [editTypes.CONFIG_CSV_SETTING]: 'kylinLang.common.prev',
  [editTypes.CONFIG_CSV_STRUCTURE]: 'kylinLang.common.prev',
  [editTypes.CONFIG_CSV_SQL]: 'kylinLang.common.prev',
  [editTypes.VIEW_SOURCE]: '',
  [editTypes.HIVE]: 'kylinLang.common.prev',
  [editTypes.RDBMS]: '',
  [editTypes.RDBMS2]: '',
  [editTypes.KAFKA]: ''
}

export const confirmMaps = {
  [editTypes.SELECT_SOURCE]: 'kylinLang.common.next',
  [editTypes.CONFIG_SOURCE]: 'kylinLang.common.next',
  [editTypes.VIEW_SOURCE]: 'kylinLang.common.ok',
  [editTypes.CSV]: 'kylinLang.common.next',
  [editTypes.CONFIG_CSV_SETTING]: 'kylinLang.common.next',
  [editTypes.CONFIG_CSV_STRUCTURE]: 'kylinLang.common.submit',
  [editTypes.CONFIG_CSV_SQL]: 'kylinLang.common.submit',
  [editTypes.HIVE]: 'kylinLang.common.loadMetadata',
  [editTypes.RDBMS]: 'kylinLang.common.sync',
  [editTypes.RDBMS2]: 'kylinLang.common.sync',
  [editTypes.KAFKA]: 'kylinLang.common.submit',
  [editTypes.SETTING]: 'kylinLang.common.save'
}

function _getSelectSourceSubmitData (form) {
  const sourceType = String(form.project.override_kylin_properties['kylin.source.default'])
  return {
    project: form.project.name,
    source_type: sourceType
  }
}

function _getLoadTableSubmitData (form) {
  return {
    project: form.project.name,
    datasource_type: form.project.override_kylin_properties['kylin.source.default'],
    tables: form.selectedTables,
    databases: form.selectedDatabases,
    need_sampling: form.needSampling,
    sampling_rows: form.samplingRows
  }
}

function _getKafkaSubmitData (form) {
  return {}
}

// 专家模式创建csv数据源
function _getExpertModelCreateCsv (form) {
  let csvSetting = form.csvSettings
  return {
    credential: collectObject(csvSetting, ['type', 'accessKey', 'secretKey'], true),
    project: form.project.name,
    ...collectObject(csvSetting, ['url', 'separatorChar', 'quoteChar', 'ddl', 'type'], false, true)
  }
}
// 向导模式创建csv数据源
function _getGuideModelCreateCsv (form) {
  let csvSetting = form.csvSettings
  return {
    credential: collectObject(csvSetting, ['type', 'accessKey', 'secretKey'], true),
    project: form.project.name,
    tableData: JSON.stringify(csvSetting.tableData),
    ...collectObject(csvSetting, ['url', 'separatorChar', 'quoteChar', 'ddl', 'type'], false, true)
  }
}

export function getSubmitData (form, editType) {
  switch (editType) {
    case editTypes.SELECT_SOURCE:
      return _getSelectSourceSubmitData(form)
    case editTypes.HIVE:
    case editTypes.RDBMS:
    case editTypes.RDBMS2:
      return _getLoadTableSubmitData(form)
    case editTypes.KAFKA:
      return _getKafkaSubmitData(form)
    case editTypes.CONFIG_CSV_SQL:
      return _getExpertModelCreateCsv(form)
    case editTypes.CONFIG_CSV_STRUCTURE:
      return _getGuideModelCreateCsv(form)
  }
}
