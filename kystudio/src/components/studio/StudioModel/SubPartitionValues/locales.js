export default {
  'en': {
    subParValuesTitle: 'Subpartition Values',
    searchPlaceholder: 'Search by subpartition value',
    segmentAmount: 'Segment Amount',
    segmentAmountTips: 'Amount of the segments built with this subpartition value / Total amount',
    addSubParValueTitle: 'Add Subpartition Value',
    addSubParValueDesc: 'The added values could be selected when building indexes for subpartitions.',
    multiPartitionPlaceholder: 'Please import, use comma (,) to separate multiple values',
    deleteSubPartitionValuesTitle: 'Delete Subpartition Value',
    deleteSubPartitionValuesTip: '{subSegsLength} subpartition(s) are selected. The deleted values couldn\'t be selected when building indexes for subpartitions. <br>Are you sure you want to delete?',
    deleteSubPartitionValuesByBuild: '{subSegsLength} subpartition(s) are selected. The deleted values could\'t be selected when building indexes for subpartitions. <br>The following {subSegsByBuildLength} subpartition(s) have been built already. The built data would be deleted as well. <br>Are you sure you want to delete?',
    duplicatePartitionValueTip: 'Some values are duplicated.',
    removeDuplicateValue: 'Clear invalid values'
  },
  'zh-cn': {
    subParValuesTitle: '子分区值',
    searchPlaceholder: '搜索子分区值',
    segmentAmount: 'Segment 数',
    segmentAmountTips: '已构建该子分区值的 Segment 数 / Segment 总数',
    addSubParValueTitle: '添加子分区值',
    addSubParValueDesc: '添加的子分区值可在构建时被快速选中，进行子分区的构建。',
    multiPartitionPlaceholder: '请输入子分区值，多个值请用逗号（,）分隔',
    deleteSubPartitionValuesTitle: '删除子分区值',
    deleteSubPartitionValuesTip: '共选择了 {subSegsLength} 个子分区值，删除后在构建时不可再被选中。<br>确定删除吗？',
    deleteSubPartitionValuesByBuild: '您共选择了 {subSegsLength} 个子分区值，删除后在构建时不可再被选中。<br>其中以下 {subSegsByBuildLength} 个子分区值在 Segment 中已构建，删除后对应的构建数据也将被删除。<br>确定删除吗？',
    duplicatePartitionValueTip: '输入值不可重复。',
    removeDuplicateValue: '清空无效的值'
  }
}
