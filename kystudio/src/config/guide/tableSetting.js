// monitor 页面的脚本配置
export function tableSettingDrama () {
  return [
    {
      eventID: 1,
      done: false,
      search: '.guideTipSetPartitionConfitmBtn'
    },
    {
      eventID: 2,
      done: false,
      search: '.guideTipSetPartitionConfitmBtn'
    },
    {
      eventID: 1,
      done: false,
      target: 'datasourceTree'
    },
    {
      eventID: 1,
      done: false,
      target: 'datasourceTree', // 飞向指定的列
      search: '.guide-ssbp_lineorder'
    }, {
      eventID: 2,
      done: false,
      target: 'datasourceTree', // 给fact table设置制定的pk
      search: '.guide-ssbp_lineorder'
    },
    {
      eventID: 1,
      done: false,
      target: 'tablePartitionColumn'
    },
    {
      eventID: 32,
      done: false,
      target: 'tablePartitionColumn',
      val: 'LO_ORDERDATE'
    },
    {
      eventID: 1,
      done: false,
      target: 'getPartitionColumnFormat'
    },
    {
      eventID: 2,
      done: false,
      target: 'getPartitionColumnFormat' // 获得分区列的时间格式
    },
    {
      eventID: 1,
      done: false,
      target: 'tableLoadDataBtn'
    },
    {
      eventID: 2,
      done: false,
      target: 'tableLoadDataBtn' // 点击加载数据按钮
    },
    {
      eventID: 1,
      done: false,
      target: 'getRangeDataBtn'
    },
    {
      eventID: 2,
      done: false,
      target: 'getRangeDataBtn' // 点击加载输入范围
    },
    {
      eventID: 21,
      done: false,
      target: 'getRangeData' // 点击加载输入范围
    },
    {
      eventID: 51,
      done: false,
      target: 'checkDataRangeHasData' // 点击选择输入范围
    },
    {
      eventID: 1,
      done: false,
      target: 'saveRangeBtn'
    },
    {
      eventID: 2,
      done: false,
      target: 'saveRangeBtn' // 保存数据加载范围
    }
  ]
}

