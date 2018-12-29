import { renderLoadHiveTables } from './generate'
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
      tip: '点击添加数据源按钮',
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
      tip: '点击选择Hive数据源',
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
      tip: '点击保存，加载数据源表',
      target: 'saveSourceType' // 鼠标移动到保存hive数据源按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'saveSourceType' // 鼠标点击保存hive数据源按钮
    },
    ...renderLoadHiveTables({
      'SSB': ['SSBCUSTOMER', 'SSBDATES', 'SSBPART', 'SSBP_LINEORDER', 'SSBSUPPLIER']
    }),
    {
      eventID: 1,
      done: false,
      tip: '点击同步加载表',
      target: 'saveSourceType' // 鼠标移动到加载数据源按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'saveSourceType' // 鼠标点击加载数据源按钮
    },
    {
      eventID: 5,
      done: false,
      target: 'selectHiveTables' // 鼠标点击加载数据源按钮
    }
  ]
}
