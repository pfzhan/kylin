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
    changePartitionTitle: 'Change Partition',
    changePartitionContent1: 'You\'re going to change the table {tableName}\'s partition from {oldPartitionKey} to {newPartitionKey}.',
    changePartitionContent2: 'The change may clean loaded storage {storageSize} and re-load data.',
    changePartitionContent3: 'Do you really need to change the partition?',
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
    changePartitionTitle: '修改分区',
    changePartitionContent1: '当前操作将把 源表 {tableName} 的分区从 {oldPartitionKey} 更换为 {newPartitionKey}。',
    changePartitionContent2: '本次修改后，本表下的 {storageSize} 数据将被清空，并按照最新的分区重新加载。',
    changePartitionContent3: '您确认需要继续修改吗？',
    remindLoadRange: 'If you have tables which increase by day, it is suggested to select the corresponding date column as partition key. Especially, tables containing historical data, in which new data is added into the newest partition.',
    loadRange: '加载范围',
    loadData: '加载数据',
    notLoadYet: '尚未载入'
  }
}
