export default {
  'en': {
    partitionSet: 'Partition Setting',
    modelPartitionSet: 'Model Partition',
    partitionDateColumn: 'Time Partition Column',
    noPartition: 'No Partition',
    dateFormat: 'Time Format',
    detectFormat: 'Detect partition format',
    noColumnFund: 'Column not found',
    pleaseInputColumn: 'Please select a partition column',
    changeSegmentTip1: 'You have modified the partition column as {tableColumn}, time format {dateType}. After saving, all segments under the model {modelName} will be purged. You need to reload the data, the model cannot serve related queries during data loading. Please confirm whether to submit?',
    changeSegmentTip2: 'You have modified as no partition column. After saving, all segments under the model {modelName} will be purged . The system will automatically rebuild the index and full load the data. The model cannot serve related queries during index building. Please confirm whether to submit?',
    changeSegmentTips: 'Model partition has changed. Once saving the model, all segments and data will be deleted. As a result, this model CAN’T be used to serve queries.<br/>Do you want to continue?'
  },
  'zh-cn': {
    partitionSet: '分区设置',
    modelPartitionSet: '分区设置',
    partitionDateColumn: '时间分区列',
    noPartition: '无分区',
    dateFormat: '时间格式',
    detectFormat: '获取分区格式',
    pleaseInputColumn: '请选择分区列',
    noColumnFund: '找不到该列',
    changeSegmentTip1: '您修改分区列为 {tableColumn}，格式为 {dateType}，保存后会导致模型 {modelName} 下的所有 Segments 被清空。您需要重新加载数据，数据加载期间该模型不能服务于相关的查询。请确认是否提交？',
    changeSegmentTip2: '您修改为无分区列，保存后会导致模型 {modelName} 下所有 Segments 被清空。系统将自动重新构建索引并全量加载数据，索引构建期间该模型不能服务于相关的查询。请确认是否提交？',
    changeSegmentTips: '模型分区设置发生改变，保存后系统将删除所有 Segment 及数据。模型将无法服务于业务查询。<br/>是否要继续保存？'
  }
}
