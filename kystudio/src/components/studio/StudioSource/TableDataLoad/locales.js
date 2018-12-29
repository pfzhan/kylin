export default {
  'en': {
    tableName: 'Table Name:',
    partitionKey: 'Partition Key:',
    loadInterval: 'Load Interval:',
    storageType: 'Storage Type:',
    storageSize: 'Storage Size:',
    totalRecords: 'Total Records:',
    rows: 'Rows',
    noPartition: 'No Partition',
    changePartitionCost: 'There are {modelCount} models using this table as fact table, so change the table\'s load type will have a damaging impact on their existing data. {storageSize} data would be invalid instantly and need to reload with new loading settings. Do you really need to change the load type for this table?',
    remindLoadRange: 'If you have tables which increase by day, it is suggested to select the corresponding date column as partition key. Especially, tables containing historical data, in which new data is added into the newest partition.',
    loadRange: 'Loaded Range',
    loadData: 'Load Data',
    notLoadYet: 'Not loaded yet'
  },
  'zh-cn': {
    tableName: '表名：',
    partitionKey: '分区列：',
    loadInterval: '加载间隔：',
    storageType: '存储类型：',
    storageSize: '占用大小：',
    totalRecords: '数据数量：',
    rows: '行',
    noPartition: '无分区列',
    changePartitionCost: 'There are {modelCount} models using this table as fact table, so change the table\'s load type will have a damaging impact on their existing data. {storageSize} data would be invalid instantly and need to reload with new loading settings. Do you really need to change the load type for this table?',
    remindLoadRange: 'If you have tables which increase by day, it is suggested to select the corresponding date column as partition key. Especially, tables containing historical data, in which new data is added into the newest partition.',
    loadRange: '加载范围',
    loadData: '加载数据',
    notLoadYet: '尚未载入'
  }
}
