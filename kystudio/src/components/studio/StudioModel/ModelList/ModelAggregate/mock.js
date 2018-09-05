export const aggregateGroups = [
  {
    includes: ['KYLIN_ACCOUNT.ACCOUNT_ID', 'KYLIN_SALES.SELLER_ID'],
    select_rule: {
      hierarchy_dims: [],
      mandatory_dims: [],
      joint_dims: [],
      dim_cap: 0
    }
  },
  {
    includes: ['KYLIN_ACCOUNT.ACCOUNT_ID', 'KYLIN_SALES.SELLER_ID'],
    select_rule: {
      hierarchy_dims: [],
      mandatory_dims: ['KYLIN_ACCOUNT.ACCOUNT_ID'],
      joint_dims: []
    }
  },
  {
    includes: ['KYLIN_SALES.SELLER_ID'],
    select_rule: {
      hierarchy_dims: [],
      mandatory_dims: ['KYLIN_SALES.SELLER_ID'],
      joint_dims: []
    }
  }
]
