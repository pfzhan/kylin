export default {
  'en': {
    partitionSet: 'Partition Setting',
    dateFormat: 'Time Format',
    partitionDateColumn: 'Time Partition Column',
    saveModel: 'Save Model',
    setting: 'Setting',
    buildRange: 'Build Range',
    startDate: 'Start Date',
    endDate: 'End Date',
    to: 'To',
    loadData: 'Load Data',
    loadExistingData: 'Load existing data',
    loadExistingDataDesc: 'Load new records existing from the last load job.',
    customLoadRange: 'Customize Load Range',
    loadRange: 'Loaded Range',
    noPartition: 'No Partition',
    invaildDate: 'Please enter a valid date',
    detectAvailableRange: 'Detect available range',
    modelPartitionSet: 'Model Partition',
    modelSaveSet: 'Save',
    dataFilterCond: 'Data Filter Condition',
    dataFilterCondTips: 'Data filter condition is an addition data filter during data loading. E.g. you can filter out those records with null values or specific records according to your business rules',
    noColumnFund: 'Column not found',
    pleaseInputColumn: 'Please select a partition column',
    pleaseInputColumnFormat: 'Please select time format',
    detectFormat: 'Detect partition time format',
    errorMsg: 'Error Message:',
    filterCondTips: 'Modifying the data filter conditions will result in all indexes under this model being rebuilt. Please modify with caution.',
    filterPlaceholder: 'Please enter your filter condition and no clause "WHERE" needed. If you have several filter conditions, you should combine them with "AND" or "OR". E.g. BUYER_ID <> 0001 AND COUNT_ITEM > 1000 OR TOTAL_PRICE = 1000',
    changeSegmentTip1: 'You have modified the partition column as {tableColumn}, time format {dateType}. After saving, all segments under the model {modelName} will be purged. You need to reload the data, the model cannot serve related queries during data loading. Please confirm whether to submit?',
    changeSegmentTip2: 'You have modified as full load. After saving, all segments under the model {modelName} will be purged . The system will automatically rebuild the index and full load the data. The model cannot serve related queries during index building. Please confirm whether to submit?',
    chooseBuildType: 'Please select a load method',
    incremental: 'Incremental Load',
    fullLoad: 'Full Load',
    incrementalTips: 'The system could incrementally load data based on the selected partition column.',
    fullLoadTips: 'The system will load all data.',
    changeBuildTypeTips: 'With partition setting changed, all segments and data would be deleted. The model couldn\'t serve queries. Meanwhile, the related ongoing jobs for building index would be discarded.',
    editCCBuildTip: 'The modified expression of computed column would be effective until all the related indexes have been built. Do you want to save and build index now? ',
    saveAndBuild: 'Save and Build',
    purgeSegmentDataTips: 'Model definition has changed. Once saving the model, all data in the segments will be deleted. As a result, this model CAN\'T be used to serve queries. We strongly recommend to reload all data (in total {storageSize}).\r\n Do you want to continue?',
    changeSegmentTips: 'With partition setting changed, all segments and data would be deleted. The model couldn\'t serve queries. Meanwhile, the related ongoing jobs for building index would be discarded.<br/>Do you want to continue?',
    saveAndLoad: 'Save and Build Index',
    partitionDateTable: 'Partition Table',
    multilevelPartition: 'Subpartition Column',
    multilevelPartitionDesc: 'A column from the selected table could be chosen. The models under this project could be partitioned by this column in addition to time partitioning. ',
    advanceSetting: 'Advanced Setting',
    addBaseIndexCheckBox: 'Add Base Indexes',
    secStorage: 'Secondary Storage',
    secStorageDesc: 'With this switch ON, the system will create a base table index, which will be sync to the secondary storage. It will improve the performance of ad-hoc query and detail query analysis scenarios.<br/>The index can\'t be deleted when the secondary storage is ON.',
    secStorageTips: 'With this switch OFF, the model\'s secondary storage data will be cleared。'
  },
  'zh-cn': {
    partitionSet: '分区设置',
    dateFormat: '时间格式',
    partitionDateColumn: '时间分区列',
    saveModel: '保存模型',
    setting: '设置',
    buildRange: '构建范围',
    startDate: '起始日期',
    endDate: '截止日期',
    to: '至',
    loadData: '加载数据',
    loadExistingData: '加载已有数据',
    loadExistingDataDesc: '加载从最后一次任务开始之后的最新的数据。',
    customLoadRange: '自定义加载数据范围',
    loadRange: '加载范围',
    noPartition: '无分区',
    invaildDate: '请输入合法的时间区间。',
    detectAvailableRange: '获取最新数据范围',
    modelPartitionSet: '分区设置',
    modelSaveSet: '保存',
    dataFilterCond: '数据筛选条件',
    dataFilterCondTips: '您可以通过数据筛选在保存模型时过滤掉空值数据或是符合特定条件的数据',
    noColumnFund: '找不到该列',
    pleaseInputColumn: '请选择分区列',
    pleaseInputColumnFormat: '请选择时间格式',
    detectFormat: '获取分区列时间格式',
    errorMsg: '错误信息：',
    filterCondTips: '修改数据筛选条件将会导致该模型下所有索引重新构建，请谨慎修改。',
    filterPlaceholder: '请输入过滤条件，多个条件使用 AND 或 OR 连接，不需要写 WHERE。例如：BUYER_ID <> 0001 AND COUNT_ITEM > 1000 OR TOTAL_PRICE = 1000',
    changeSegmentTip1: '您修改分区列为 {tableColumn}，格式为 {dateType}，保存后会导致模型 {modelName} 下的所有 Segments 被清空。您需要重新加载数据，数据加载期间该模型不能服务于相关的查询。请确认是否提交？',
    changeSegmentTip2: '您修改为全量加载，保存后会导致模型 {modelName} 下所有 Segments 被清空。系统将自动重新构建索引并全量加载数据，索引构建期间该模型不能服务于相关的查询。请确认是否提交？',
    chooseBuildType: '请选择加载方式',
    incremental: '增量加载',
    fullLoad: '全量加载',
    incrementalTips: '系统可以根据您选择的分区列，增量加载数据。',
    fullLoadTips: '系统会全量加载所有数据。',
    changeBuildTypeTips: '修改模型分区设置后，系统将删除所有 Segment 及数据，模型将无法服务于业务查询。同时正在执行的构建任务将被终止。',
    editCCBuildTip: '修改的可计算列表达式需待索引构建完成后生效。是否继续保存并构建索引？',
    saveAndBuild: '保存并构建',
    purgeSegmentDataTips: '模型定义关系发生改变，保存后系统将删除所有 Segment 中的数据。模型将无法服务于业务查询。为了模型能够服务于业务查询，建议您重新加载模型下所有数据（共计 {storageSize}）。\r\n是否要继续保存？',
    changeSegmentTips: '修改模型分区设置后，系统将删除所有 Segment 及数据，模型将无法服务于业务查询。同时正在执行的构建任务将被终止。<br/>是否要继续保存？',
    saveAndLoad: '保存并构建索引',
    partitionDateTable: '分区表',
    multilevelPartition: '子分区列',
    multilevelPartitionDesc: '可选择表上的一列作为子分区，对模型进行分区管理。',
    advanceSetting: '高级设置',
    addBaseIndexCheckBox: '添加基础索引',
    secStorage: '二级存储',
    secStorageDesc: '开启后系统将为模型创建一个基础明细索引。二级存储用于同步该索引数据，以提高多维度灵活查询和明细查询的查询性能。<br/>且在开启二级存储时该索引不可删除。',
    secStorageTips: '关闭后，模型的二级存储数据将被清空，可能会影响查询效率。'
  }
}
