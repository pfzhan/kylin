export default {
  'en': {
    dataSourceTypeCheckTip: 'Select Data Source',
    singleSourceTip: 'You can choose one type of data source within the project and it cannot be modified after selection.',
    upcoming: 'Coming soon',
    disabledHiveOrKafkaTips: 'Hive/Kafka data source can\'t be used when {jdbcName} table has been added. ',
    disabledJDBCTips: '{jdbcName} data source can\'t be used when Hive/Kafka table has been added. '
  },
  'zh-cn': {
    dataSourceTypeCheckTip: '选择数据源',
    singleSourceTip: '每个项目支持一种数据源类型，选定后将无法修改。',
    upcoming: '研发中',
    disabledHiveOrKafkaTips: '已添加 {jdbcName} 表时，暂无法使用 Hive/Kafka 数据源。',
    disabledJDBCTips: '已添加 Hive/Kafka 表时，暂无法使用 {jdbcName} 数据源。'
  }
}
