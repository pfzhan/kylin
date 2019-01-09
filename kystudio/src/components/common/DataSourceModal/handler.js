import { set } from '../../../util'
import { sourceTypes } from '../../../config'

export const editTypes = {
  SELECT_SOURCE: 'selectSource',
  CONFIG_SOURCE: 'configSource',
  VIEW_SOURCE: 'viewSource',
  ...sourceTypes
}

export const titleMaps = {
  [editTypes.SELECT_SOURCE]: 'selectSource',
  [editTypes.CONFIG_SOURCE]: 'configSource',
  [editTypes.VIEW_SOURCE]: 'viewSource',
  [editTypes.HIVE]: 'loadhiveTables',
  [editTypes.RDBMS]: 'loadTables',
  [editTypes.RDBMS2]: 'loadTables',
  [editTypes.KAFKA]: 'loadKafkaTopic'
}

export const cancelMaps = {
  [editTypes.SELECT_SOURCE]: 'kylinLang.common.cancel',
  [editTypes.CONFIG_SOURCE]: 'kylinLang.common.prev',
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
  [editTypes.HIVE]: 'kylinLang.common.sync',
  [editTypes.RDBMS]: 'kylinLang.common.sync',
  [editTypes.RDBMS2]: 'kylinLang.common.sync',
  [editTypes.KAFKA]: 'kylinLang.common.submit',
  [editTypes.SETTING]: 'kylinLang.common.save'
}

function _getSelectSourceSubmitData (form) {
  const sourceType = String(form.project.override_kylin_properties['kylin.source.default'])
  const properties = { ...form.project.override_kylin_properties }
  properties['kylin.source.default'] = sourceType
  const projectDesc = JSON.stringify(set(form.project, 'override_kylin_properties', properties))
  return {
    name: form.project.name,
    desc: projectDesc
  }
}

function _getLoadTableSubmitData (form) {
  return {
    projectName: form.project.name,
    sourceType: form.project.override_kylin_properties['kylin.source.default'],
    tableNames: form.selectedTables,
    databaseNames: form.selectedDatabases
  }
}

function _getKafkaSubmitData (form) {
  return {}
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
  }
}
