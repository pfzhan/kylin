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
    pleaseInputColumnFormat: 'Please select or enter a customized time format',
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
    onlyAddLeftJoinTip: 'The model definition relationship has changed and the current change only affects incremental data. To overwrite the stock data, go to the Segment page to clear the old data and rebuild.',
    changeSegmentTips: 'With partition setting changed, all segments and data would be deleted. The model couldn\'t serve queries. Meanwhile, the related ongoing jobs for building index would be discarded.<br/>Do you want to continue?',
    saveAndLoad: 'Save and Build Index',
    partitionDateTable: 'Partition Table',
    multilevelPartition: 'Subpartition Column',
    multilevelPartitionDesc: 'A column from the selected table could be chosen. The models under this project could be partitioned by this column in addition to time partitioning. ',
    advanceSetting: 'Advanced Setting',
    addBaseIndexCheckBox: 'Add Base Indexes',
    secStorage: 'Tiered Storage',
    secStorageDesc: 'With this switch ON, the system will create a base table index, which will be sync to the tiered storage. It will improve the performance of ad-hoc query and detail query analysis scenarios.<br/>The index can\'t be deleted when the tiered storage is ON.',
    secStorageTips: 'With this switch OFF, the model\'s tiered storage data will be cleared。',
    openSecStorageTips: 'It\'s recommended to turn on the tiered storage, as too many dimensions are included.',
    openSecStorageTips2: 'With the tiered storage ON, the existing data needs to be loaded to tiered storage to take effect.',
    disableSecStorageActionTips: 'The tiered storage can\'t be used for fusion or streaming models at the moment.',
    disableSecStorageActionTips2: 'The tiered storage can\'t be used because no dimension or measure has been added and the base table index can\'t be added.',
    forbidenComputedColumnTips: 'The parquet files containing data prior to 1970 cannot be loaded. <a class="ky-a-like" href="https://docs.kyligence.io/books/v4.5/en/tiered_storage/" target="_blank">View the manual <i class="el-ksd-icon-spark_link_16"></i></a>',
    secondStoragePartitionTips: 'Can\'t save the model. When the model uses incremental load method and the tiered storage is ON, the time partition column must be added as a dimension.',
    streamSecStoragePartitionTips: 'Can\'t save the model. For fusion model, the time partition column must be added as a dimension.',
    baseIndexTips: 'Base indexes include all dimensions and measures of the model and automatically update as the model changes by default.',
    notBatchModelPartitionTips: 'The time partition settings can\'t be modified after the fusion model or streaming model is saved.',
    disableChangePartitionTips: 'The time partition settings can\'t be modified for fusion model and streaming model.',
    previewFormat: 'Format preview: ',
    formatRule: 'The customized time format is supported. ',
    viewDetail: 'More info',
    rule1: 'Support using some elements of yyyy, MM, dd, HH, mm, ss, SSS in positive order',
    rule2: 'Support using - (hyphen), / (slash), : (colon), English space as separator',
    rule3: 'When using unformatted letters, use a pair of \' (single quotes) to quote, i.e. \'T\' will be recognized as T'
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
    pleaseInputColumnFormat: '请选择或输入自定义时间格式',
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
    onlyAddLeftJoinTip: '模型定义关系发生改变，当前变更只影响增量数据。如需覆盖存量数据，请至 Segment 页面清空旧数据并重新构建。',
    changeSegmentTips: '修改模型分区设置后，系统将删除所有 Segment 及数据，模型将无法服务于业务查询。同时正在执行的构建任务将被终止。<br/>是否要继续保存？',
    saveAndLoad: '保存并构建索引',
    partitionDateTable: '分区表',
    multilevelPartition: '子分区列',
    multilevelPartitionDesc: '可选择表上的一列作为子分区，对模型进行分区管理。',
    advanceSetting: '高级设置',
    addBaseIndexCheckBox: '添加基础索引',
    secStorage: '分层存储',
    secStorageDesc: '开启后系统将为模型创建一个基础明细索引。分层存储用于同步该索引数据，以提高多维度灵活查询和明细查询的查询性能。<br/>且在开启分层存储时该索引不可删除。',
    secStorageTips: '关闭后，模型的分层存储数据将被清空，可能会影响查询效率。',
    openSecStorageTips: '检测到模型维度数较多，建议开启分层存储。',
    openSecStorageTips2: '开启分层存储后，现有 Segment 数据需加载到分层存储后生效。',
    disableSecStorageActionTips: '融合数据模型或流数据模型暂无法使用分层存储',
    disableSecStorageActionTips2: '模型未定义维度/度量，无法生成基础明细索引和使用分层缓存。',
    forbidenComputedColumnTips: '分层存储暂时无法加载包含 1970 年以前数据的 parquet 文件。<a class="ky-a-like" href="https://docs.kyligence.io/books/v4.5/zh-cn/tiered_storage/" target="_blank">查看手册<i class="el-ksd-icon-spark_link_16"></i></a>',
    secondStoragePartitionTips: '无法保存模型。当增量加载的模型开启分层存储时，必须将时间分区列加入维度。',
    streamSecStoragePartitionTips: '无法保存模型。融合数据模型必须将时间分区列加入模型维度。',
    baseIndexTips: '基础索引包含模型全部维度和度量，默认随着模型变化自动更新。',
    notBatchModelPartitionTips: '保存后，融合数据模型和流数据模型将无法修改时间分区设置。',
    disableChangePartitionTips: '融合数据模型和流数据模型无法修改时间分区设置。',
    previewFormat: '格式预览：',
    formatRule: '支持输入自定义时间格式，',
    viewDetail: '更多信息',
    rule1: '支持使用 yyyy, MM, dd, HH, mm, ss, SSS 中的部分要素正序组合',
    rule2: '支持使用 -（连字符）、/（斜线号）、:（冒号）、英文空格做分隔符',
    rule3: '使用无格式字母时，需用一对 \' （单引号）引用，例如：\'T\' 将被识别为 T'
  }
}
