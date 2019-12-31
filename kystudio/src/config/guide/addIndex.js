export function addIndexDrama () {
  return [
    {
      eventID: 21,
      done: false,
      target: 'moveGuidePanelBtn',
      val: {
        left: 22,
        top: 10
      }
    },
    {
      eventID: 61,
      done: false,
      target: 'scrollModelTable'
    },
    {
      eventID: 1,
      done: false,
      target: 'setDataRangeBtn'
    },
    {
      eventID: 2,
      done: false,
      target: 'setDataRangeBtn' // 点击打开设置加载范围
    },
    {
      eventID: 1,
      done: false,
      target: 'getPartitionRangeDataBtn'
    },
    {
      eventID: 2,
      done: false,
      target: 'getPartitionRangeDataBtn' // 点击加载输入范围
    },
    {
      eventID: 21,
      done: false,
      target: 'getPartitionRangeData' // 点击加载输入范围
    },
    {
      eventID: 51,
      done: false,
      target: 'checkPartitionDataRangeHasData'
    },
    {
      eventID: 1,
      done: false,
      target: 'setbuildModelRange' // 点击加载输入范围
    },
    {
      eventID: 2,
      done: false,
      target: 'setbuildModelRange'
    },
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
      target: 'aggCatchUp', // 点击选择输入范围
      val: true
    },
    {
      eventID: 31,
      done: false,
      target: 'aggCatchUp', // 点击选择输入范围
      val: true
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
    },
    {
      eventID: 1,
      done: false,
      search: '.guideTipSetAggIndexConfitmBtn' // 移动到保存Agg的按钮
    },
    {
      eventID: 2,
      done: false,
      search: '.guideTipSetAggIndexConfitmBtn' // 点击保存Agg的按钮
    },
    {
      eventID: 5,
      done: false,
      target: 'saveAggBtn' // 点击保存Agg的按钮
    }
  ]
}
