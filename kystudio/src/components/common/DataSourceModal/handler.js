import { sourceTypes } from '../../../config'

export const titleMaps = {
  [sourceTypes.NEW]: 'newDataSource',
  [sourceTypes.HIVE]: 'loadhiveTables',
  [sourceTypes.RDBMS]: 'loadTables',
  [sourceTypes.RDBMS2]: 'loadTables',
  [sourceTypes.KAFKA]: 'loadKafkaTopic',
  [sourceTypes.SETTING]: 'kylinLang.common.dataSource'
}

export const cancelMaps = {
  [sourceTypes.NEW]: 'kylinLang.common.cancel',
  [sourceTypes.HIVE]: '',
  [sourceTypes.RDBMS]: '',
  [sourceTypes.RDBMS2]: '',
  [sourceTypes.KAFKA]: '',
  [sourceTypes.SETTING]: 'kylinLang.common.cancel'
}

export const confirmMaps = {
  [sourceTypes.NEW]: 'kylinLang.common.next',
  [sourceTypes.HIVE]: 'kylinLang.common.sync',
  [sourceTypes.RDBMS]: 'kylinLang.common.sync',
  [sourceTypes.RDBMS2]: 'kylinLang.common.sync',
  [sourceTypes.KAFKA]: 'kylinLang.common.submit',
  [sourceTypes.SETTING]: 'kylinLang.common.save'
}

export function getSubmitData (that, kafkaData) {
  const { sourceType, form, currentSelectedProject } = that

  switch (sourceType) {
    case sourceTypes.NEW:
      const project = JSON.parse(JSON.stringify(form.project))
      project.override_kylin_properties['kylin.source.default'] = String(project.override_kylin_properties['kylin.source.default'])
      return {
        name: currentSelectedProject,
        desc: JSON.stringify(project)
      }
    case sourceTypes.HIVE:
    case sourceTypes.RDBMS:
    case sourceTypes.RDBMS2:
      return {
        projectName: currentSelectedProject,
        sourceType,
        tableNames: form.selectedTables
      }
    case sourceTypes.KAFKA: {
      const data = kafkaData
      const columns = data.columnList
        .filter(column => column.checked === 'Y')
        .map((column, index) => ({
          id: index,
          name: column.name,
          datatype: column.type,
          comment: /[|]/.test(column.comment) ? column.comment : '' // 不是嵌套结构的就不传该内容
        }))
      const tableData = {
        name: data.kafkaMeta.name,
        source_type: 1,
        columns,
        database: data.database || 'Default'
      }

      const streamingConfig = {
        ...data.streamingMeta,
        name: data.kafkaMeta.name
      }
      return {
        kafkaConfig: JSON.stringify(data.kafkaMeta),
        streamingConfig: JSON.stringify(streamingConfig),
        project: currentSelectedProject,
        tableData: JSON.stringify(tableData)
      }
    }
    case sourceTypes.SETTING:
      return {
        isAutoUpdate: form.isAutoUpdate
      }
  }
}
