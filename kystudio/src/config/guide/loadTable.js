export function loadTableDrama () {
  return [
    {
      eventID: 5,
      done: false,
      target: 'addProjectInput' // 验证保存成功project的前置条件
    },
    {
      eventID: 1,
      done: false,
      target: 'addDatasource' // 鼠标移动到添加数据源
    },
    {
      eventID: 2,
      done: false,
      target: 'addDatasource' // 鼠标点击添加数据源
    },
    {
      eventID: 1,
      done: false,
      target: 'selectHive' // 鼠标移动到选择hive数据源
    },
    {
      eventID: 2,
      done: false,
      target: 'selectHive' // 鼠标点击选择hive数据源
    },
    {
      eventID: 1,
      done: false,
      target: 'saveSourceType' // 鼠标移动到保存hive数据源按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'saveSourceType' // 鼠标点击保存hive数据源按钮
    },
    {
      eventID: 1,
      done: false,
      target: 'hiveTree', // 展开指定的database
      search: '.guide-SSB'
    },
    {
      eventID: 7,
      done: false,
      target: 'hiveTree', // 展开指定的database
      search: '.guide-SSB'
    },
    {
      eventID: 1,
      done: false,
      target: 'hiveTree', // 展开指定的database
      search: '.guide-SSBCUSTOMER'
    },
    {
      eventID: 2,
      done: false,
      target: 'hiveTree', // 展开指定的database
      search: '.guide-SSBCUSTOMER'
    },
    {
      eventID: 1,
      done: false,
      target: 'hiveTree', // 展开指定的database
      search: '.guide-SSBDATES'
    },
    {
      eventID: 2,
      done: false,
      target: 'hiveTree', // 展开指定的database
      search: '.guide-SSBDATES'
    },
    {
      eventID: 1,
      done: false,
      target: 'hiveTree', // 展开指定的database
      search: '.guide-SSBP_LINEORDER'
    },
    {
      eventID: 2,
      done: false,
      target: 'hiveTree', // 展开指定的database
      search: '.guide-SSBP_LINEORDER'
    },
    {
      eventID: 1,
      done: false,
      target: 'hiveTree', // 展开指定的database
      search: '.guide-SSBPART'
    },
    {
      eventID: 2,
      done: false,
      target: 'hiveTree', // 展开指定的database
      search: '.guide-SSBPART'
    },
    {
      eventID: 1,
      done: false,
      target: 'hiveTree', // 展开指定的database
      search: '.guide-SSBSUPPLIER'
    },
    {
      eventID: 2,
      done: false,
      target: 'hiveTree', // 展开指定的database
      search: '.guide-SSBSUPPLIER'
    },
    {
      eventID: 1,
      done: false,
      target: 'saveSourceType' // 鼠标移动到加载数据源按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'saveSourceType' // 鼠标点击加载数据源按钮
    }
  ]
}
