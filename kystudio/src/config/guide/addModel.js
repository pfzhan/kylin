import { renderModelAddTableData, renderDimensionData, renderMeasuresData } from './generate'
export function addModelDrama () {
  return [
    {
      eventID: 5,
      done: false,
      target: 'hiveTree', // 鼠标点击加载数据源按钮
      group: 1
    },
    {
      eventID: 8,
      done: false,
      router: 'ModelList', // 跳转到模型列表页面
      group: 1
    },
    {
      eventID: 1,
      done: false,
      tip: 'addModelTip',
      target: 'addModelBtn', // 移动到模型添加按钮
      group: 1
    },
    {
      eventID: 2,
      done: false,
      target: 'addModelBtn', // 模拟点击模型添加按钮
      group: 1
    },
    {
      eventID: 1,
      done: false,
      target: 'inputModelName', // 移动到模型名称输入框
      group: 1
    },
    {
      eventID: 3,
      done: false,
      target: 'inputModelName', // 输入模型名称
      val: 'test_model_name' + Date.now(),
      group: 1
    },
    {
      eventID: 1,
      done: false,
      target: 'inputModelDesc', // 移动到模型描述输入框
      group: 1
    },
    {
      eventID: 3,
      done: false,
      target: 'inputModelDesc', // 输入模型描述
      val: 'test_model_desc',
      group: 1
    },
    {
      eventID: 1,
      done: false,
      target: 'addModelSave', // 移动到模型保存按钮
      group: 1
    },
    {
      eventID: 2,
      done: false,
      target: 'addModelSave' // 点击模型添加跳转按钮
    },
    {
      eventID: 1,
      done: false,
      tip: 'addModelTip1',
      target: 'modelDataSourceTreeScrollBox' // 进入可视区域
    },
    // 添加表SSB.P_LINEORDER
    // 添加表DATES.D_DATEKEY
    ...renderModelAddTableData([
      {
        name: 'SSB.P_LINEORDER',
        x: 900,
        y: 200
      },
      {
        name: 'SSB.DATES',
        x: 600,
        y: 519
      },
      {
        name: 'SSB.CUSTOMER',
        x: 850,
        y: 519
      }, {
        name: 'SSB.SUPPLIER',
        x: 1100,
        y: 519
      }, {
        name: 'SSB.PART',
        x: 1350,
        y: 519
      }
    ], [{
      ftable: 'SSB.P_LINEORDER',
      ptable: 'SSB.DATES',
      fcolumn: 'LO_ORDERDATE',
      pcolumn: 'D_DATEKEY',
      joinType: 'LEFT'
    }, {
      ftable: 'SSB.P_LINEORDER',
      ptable: 'SSB.CUSTOMER',
      fcolumn: 'LO_CUSTKEY',
      pcolumn: 'C_CUSTKEY',
      joinType: 'LEFT'
    }, {
      ftable: 'SSB.P_LINEORDER',
      ptable: 'SSB.SUPPLIER',
      fcolumn: 'LO_SUPPKEY',
      pcolumn: 'S_SUPPKEY',
      joinType: 'LEFT'
    }, {
      ftable: 'SSB.P_LINEORDER',
      ptable: 'SSB.PART',
      fcolumn: 'LO_PARTKEY',
      pcolumn: 'P_PARTKEY',
      joinType: 'LEFT'
    }], 'SSB.P_LINEORDER'),
    // end
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
      eventID: 1,
      done: false,
      target: 'modelActionPanel', // 飞向模型表操作区域
      search: '.switch'
    },
    {
      eventID: 2,
      done: false,
      target: 'modelActionPanel', // 点击切换模型类型
      search: '.switch'
    },
    {
      eventID: 1,
      done: false,
      target: 'actionTable', // 飞向设置
      search: '.guide-setting'
    },
    {
      eventID: 2,
      done: false,
      target: 'actionTable', // 点击设置 去除蒙版
      search: '.guide-setting'
    },
    {
      eventID: 1,
      done: false,
      tip: 'addModelTip2',
      target: 'dimensionPanelShowBtn' // 飞向dimension 面板触发按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'dimensionPanelShowBtn' // 飞向dimension 面板触发按钮
    },
    {
      eventID: 1,
      done: false,
      target: 'batchAddDimension' // 飞向dimension 批量弹出按钮按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'batchAddDimension' // 点击dimension 批量弹出按钮按钮
    },
    ...renderDimensionData([
      'P_LINEORDER.LO_ORDERDATE',
      'P_LINEORDER.LO_CUSTKEY',
      'P_LINEORDER.LO_SUPPKEY',
      'P_LINEORDER.LO_PARTKEY',
      'DATES.D_DATEKEY',
      'CUSTOMER.C_CUSTKEY',
      'SUPPLIER.S_SUPPKEY',
      'PART.P_PARTKEY'
    ]),
    {
      eventID: 1,
      done: false,
      target: 'saveBatchDimensionBtn' // 飞向保存批量dimension的按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'saveBatchDimensionBtn' // 点击保存批量dimension的按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'moveGuidePanelBtn', // 移动guide面板
      val: {
        left: 22,
        top: 10
      }
    },
    {
      eventID: 1,
      done: false,
      target: 'measurePanelShowBtn' // 飞向打开measure面板的按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'measurePanelShowBtn' // 点击展开measure面板的按钮
    },
    ...renderMeasuresData([{
      expression: 'SUM(column)',
      parameter: 'P_LINEORDER.LO_REVENUE'
    }, {
      expression: 'SUM(column)',
      parameter: 'P_LINEORDER.LO_SUPPLYCOST'
    }, {
      expression: 'SUM(column)',
      parameter: 'P_LINEORDER.V_REVENUE'
    }, {
      expression: 'COUNT_DISTINCT',
      parameter: 'P_LINEORDER.LO_LINENUMBER'
    }]),
    {
      eventID: 2,
      done: false,
      target: 'moveGuidePanelBtn', // 恢复面板位置
      val: null
    },
    // 保存模型
    {
      eventID: 1,
      done: false,
      tip: 'addModelTip3',
      target: 'saveModelBtn' // 飞向保存模型
    },
    {
      eventID: 2,
      done: false,
      target: 'saveModelBtn' // 点击保存模型
    },
    {
      eventID: 1,
      done: false,
      target: 'partitionTable' // 飞向partition table
    },
    {
      eventID: 31,
      done: false,
      target: 'partitionTable', // 输入partition table
      val: 'P_LINEORDER'
    },
    {
      eventID: 1,
      done: false,
      target: 'partitionColumn' // 飞向partition column
    },
    {
      eventID: 31,
      done: false,
      target: 'partitionColumn', // 输入partition column内容
      val: 'LO_ORDERDATE'
    },
    {
      eventID: 1,
      done: false,
      target: 'partitionSaveBtn' // 飞向partition保存按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'partitionSaveBtn'// 点击partition保存按钮
    },
    {
      eventID: 1,
      done: false,
      tip: 'addModelTip4',
      target: 'willAddIndex' // 飞向添加index 按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'willAddIndex' // 点击添加index按钮
    }
  ]
}
