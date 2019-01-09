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
      eventID: 31,
      done: false,
      target: 'tablePartitionColumn',
      val: 'LO_ORDERDATE'
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
      target: 'checkloadDataRangeRaido'
    },
    {
      eventID: 31,
      done: false,
      target: 'checkloadDataRangeRaido', // 点击选择输入范围
      val: true
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

