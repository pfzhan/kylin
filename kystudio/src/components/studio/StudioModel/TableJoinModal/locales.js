export default {
  'en': {
    addJoinCondition: 'Add Join Relationship',
    checkCompleteLink: 'connection info is incomplete',
    delConn: 'Delete connection',
    delConnTip: 'Are you sure you want to delete this connection?',
    noColumnFund: 'Column not found',
    tableJoin: 'Join Relationship for Tables',
    columnsJoin: 'Join Relationship for Columns',
    joinNotice: 'Join relationships must meet the following criteria. ',
    joinErrorNotice: 'The join condition can\'t be saved. Please modify and try again.',
    details: 'View Details',
    notice1: 'Can\'t define multiple join conditions for the same columns',
    notice2: 'Join relationship ≥ and < must be used in pairs, and same column must be joint in both conditions',
    notice3: 'Join relationship for columns should include at least one equal-join condition (=)',
    notice4: 'Two tables could only be joined by the same condition for one time',
    manyToOne: 'One-to-One or Many-to-One',
    oneToOne: 'One-to-One',
    oneToMany: 'One-to-Many',
    manyToMany: 'One-to-Many or Many-to-Many',
    tableRelationTips: 'When the relationship is one-to-many or many-to-many, the columns which are not defined as dimensions from the joined table {tableName} can\'t be queried.',
    tableRelation: 'Table Relationship',
    precomputeJoin: 'Precompute Join Relationships',
    precomputeJoinTip: '* If precomputing join relationships, the columns which are not defined as dimensions from the joined dimension table can’t be queried. <br/> * If not precomputing join relationships, the storage and operation cost would be reduced. However, the columns from the joined dimension table can\'t used for dimensions, measures, computed columns and indexes of the model. Query can be served by adding the join key to the aggregate group. ',
    disabledPrecomputeJoinTip: 'Precomputing the join relationships is not allowed. Because the joined dimension table is in the excluded rule of recommendation setting, precomputing may cause mistakes in query results. ',
    backEdit: 'Continue Editing',
    deletePrecomputeJoinDialogTips: 'If not precomputing the join relationships, the columns from the joined dimension table can’t be used for dimensions, measures, computed columns and indexes of the model. The following data will be deleted when the model is saved：',
    measureCollapse: 'Measures ({num})',
    dimensionCollapse: 'Dimensions ({num})',
    computedColumnCollapse: 'Computed Columns ({num})',
    indexesCollapse: 'Indexes ({num})',
    aggIndexes: 'Aggregate Indexes ({len})',
    tableIndexes: 'Table Indexes ({len})',
    noFactTableTips: 'Unable to add join relationship between dimension tables. Please add join relationship of fact table first.'
  },
  'zh-cn': {
    addJoinCondition: '添加关联关系',
    checkCompleteLink: '连接信息不完整',
    delConn: '删除连线',
    delConnTip: '确认删除该连接关系吗？',
    noColumnFund: '找不到该列',
    tableJoin: '表的关联关系',
    columnsJoin: '列的关联关系',
    joinNotice: '关联关系需满足以下条件，',
    joinErrorNotice: '关联关系无法保存，请修改并重试。',
    details: '查看详情',
    notice1: '列的关联关系不可重复定义',
    notice2: '关联关系 ≥ 和 < 必须成对出现，且位于中间的列必须一致',
    notice3: '列必须至少包含一个 = 的关联关系',
    notice4: '两张表仅可使用同样的条件连接一次',
    manyToOne: '一对一或多对一',
    oneToOne: '一对一',
    oneToMany: '一对多',
    manyToMany: '一对多或多对多',
    tableRelationTips: '当表关系为一对多或多对多时，将无法查询被关联表 {tableName} 的非维度列。',
    tableRelation: '表关系',
    precomputeJoin: '预计算关联关系',
    precomputeJoinTip: '* 预计算关联关系将导致关联维度表中的非维度列无法查询。<br/> * 不进行预计算可降低存储和运维成本。但关联维度表中的列将无法在模型的维度，度量，可计算列和索引使用，可将关联键加入聚合组来服务查询。',
    disabledPrecomputeJoinTip: '不允许预计算关联关系。因为该关联维度表已加入优化建议屏蔽设置，预计算可能导致查询结果出错。',
    backEdit: '回到编辑',
    deletePrecomputeJoinDialogTips: '不进行预计算时，关联维度表中的列将无法在模型的维度，度量，可计算列和索引中使用。模型保存时，以下数据将被删除：',
    measureCollapse: '度量 ({num})',
    dimensionCollapse: '维度（{num}）',
    computedColumnCollapse: '可计算列（{num}）',
    indexesCollapse: '索引（{num}）',
    aggIndexes: '聚合索引（{len}）',
    tableIndexes: '明细索引（{len}）',
    noFactTableTips: '无法添加维度表之间的关联关系，请先添加事实表的关联关系。'
  }
}