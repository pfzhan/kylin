export function addIndexDrama () {
  return [
    {
      eventID: 1,
      done: false,
      target: 'addAggBtn' // 点击保存模型
    },
    {
      eventID: 2,
      done: false,
      target: 'addAggBtn' // 点击保存模型
    },
    {
      eventID: 1,
      done: false,
      target: 'aggIncludes' // 移动到includes 输入框
    },
    {
      eventID: 2,
      done: false,
      target: 'selectAllIncludesBtn' // 点击全选按钮
    },
    {
      eventID: 1,
      done: false,
      target: 'aggMandatory' // 移动到Mandatory 输入框
    },
    {
      eventID: 32,
      done: false,
      target: 'aggMandatory', // 输入Mandatory
      val: ['P_LINEORDER.LO_CUSTKEY']
    },
    {
      eventID: 1,
      done: false,
      target: 'aggHierarchy' // 移动到Hierarchy 输入框
    },
    {
      eventID: 32,
      done: false,
      target: 'aggHierarchy', // 输入Hierarchy
      val: ['P_LINEORDER.LO_PARTKEY', 'PART.P_PARTKEY', 'SUPPLIER.S_SUPPKEY']
    },
    {
      eventID: 1,
      done: false,
      target: 'joint' // 移动到Joint输入框
    },
    {
      eventID: 32,
      done: false,
      target: 'joint', // 输入Joint
      val: ['P_LINEORDER.LO_ORDERDATE', 'P_LINEORDER.LO_SUPPKEY']
    },
    {
      eventID: 1,
      done: false,
      target: 'saveAggBtn' // 移动到保存Agg的按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'saveAggBtn' // 点击保存Agg的按钮
    }
  ]
}
