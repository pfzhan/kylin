export default {
  'en': {
    partitionSet: 'Partition Setting',
    modelPartitionSet: 'Model Partition',
    partitionDateTable: 'Partition Table',
    partitionDateColumn: 'Time Partition Column',
    multilevelPartition: 'Subpartition Column',
    multilevelPartitionDesc: 'A column from the selected table could be chosen. The models under this project could be partitioned by this column in addition to time partitioning. ',
    segmentChangedTips: 'With partition setting changed, all segments and data would be deleted. The model couldn’t serve queries. Meanwhile, the related ongoing jobs for building index would be discarded.',
    noPartition: 'No Partition',
    dateFormat: 'Time Format',
    detectFormat: 'Detect partition time format',
    noColumnFund: 'Column not found',
    pleaseInputColumn: 'Please select time format',
    changeSegmentTip1: 'You have modified the partition column as {tableColumn}, time format {dateType}. After saving, all segments under the model {modelName} will be purged. You need to reload the data, the model cannot serve related queries during data loading. Please confirm whether to submit?',
    changeSegmentTip2: 'You have modified as no partition column. After saving, all segments under the model {modelName} will be purged . The system will automatically rebuild the index and full load the data. The model cannot serve related queries during index building. Please confirm whether to submit?',
    changeSegmentTips: 'With partition setting changed, all segments and data would be deleted. The model couldn’t serve queries. Meanwhile, the related ongoing jobs for building index would be discarded.<br/>Do you want to continue?'
  },
  'zh-cn': {
    partitionSet: '分区设置',
    modelPartitionSet: '分区设置',
    partitionDateTable: '分区表',
    partitionDateColumn: '时间分区列',
    multilevelPartition: '子分区列',
    multilevelPartitionDesc: '可选择表上的一列作为子分区，对模型进行分区管理。',
    segmentChangedTips: '修改模型分区设置后，系统将删除所有 Segment 及数据，模型将无法服务于业务查询。同时正在执行的构建任务将被终止。',
    noPartition: '无分区',
    dateFormat: '时间格式',
    detectFormat: '获取分区列时间格式',
    pleaseInputColumn: '请选择时间格式',
    noColumnFund: '找不到该列',
    changeSegmentTip1: '您修改分区列为 {tableColumn}，格式为 {dateType}，保存后会导致模型 {modelName} 下的所有 Segments 被清空。您需要重新加载数据，数据加载期间该模型不能服务于相关的查询。请确认是否提交？',
    changeSegmentTip2: '您修改为无分区列，保存后会导致模型 {modelName} 下所有 Segments 被清空。系统将自动重新构建索引并全量加载数据，索引构建期间该模型不能服务于相关的查询。请确认是否提交？',
    changeSegmentTips: '修改模型分区设置后，系统将删除所有 Segment 及数据，模型将无法服务于业务查询。同时正在执行的构建任务将被终止。<br/>确定继续吗？'
  }
}
