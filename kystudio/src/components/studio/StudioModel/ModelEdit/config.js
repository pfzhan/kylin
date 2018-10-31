let baseIndex = 100
let rootBox = '.model-edit-outer'
export const modelRenderConfig = {
  jsPlumbAnchor: [
    [0.5, 0, 0.6, 0],
    [0.1, 0, 0.2, 0],
    [0.8, 0, 0.9, 0],
    [0.5, 1, 0.6, 1],
    [0.1, 1, 0.2, 1],
    [0.8, 1, 0.9, 1],
    [0, 0.8, 0, 0.9],
    [1, 0.8, 1, 0.9]
  ], // 连线动态附着点设置
  baseLeft: 100, // 可视区域距离画布最左侧距离
  baseTop: 10, // 可视区域距离画布最顶部距离
  tableBoxWidth: 220, // table盒子宽度
  tableBoxHeight: 195, // table盒子高度
  tableBoxLeft: 50, // table盒子相对于左侧兄弟元素距离
  tableBoxTop: 50, // table盒子相对于顶部兄弟元素距离
  zoom: 8,
  rootBox: rootBox, // 根元素
  drawBox: '.model-edit', // 绘制区域
  joinKind: {
    inner: 'INNER',
    left: 'LEFT'
  },
  joinKindSelectData: [{label: 'Inner Join', value: 'INNER'}, {label: 'Left Join', value: 'LEFT'}],
  columnType: ['D', 'M', '－'],
  tableKind: {
    rootFact: 'ROOTFACT',
    fact: 'FACT',
    lookup: 'LOOKUP'
  },
  searchKeys: {
    join: ['left', 'left join', 'inner', 'inner join']

  },
  searchAction: {
    table: [{action: 'tableeditjoin', i18n: 'editjoin'}, {action: 'tableaddjoin', i18n: 'tableaddjoin'}], // [{action: 'showtable', i18n: 'showtable'}], // 搜索table
    column: [{action: 'adddimension', i18n: 'adddimension'}, {action: 'addmeasure', i18n: 'addmeasure'}, {action: 'addjoin', i18n: 'addjoin'}], // 搜索列
    measure: [{action: 'editmeasure', i18n: 'editmeasure'}], // 搜索measure
    dimension: [{action: 'editdimension', i18n: 'editdimension'}], // 搜索dimension
    join: [{action: 'editjoin', i18n: 'editjoin'}] //  搜索join
  },
  searchCountLimit: 4, // 搜索每一类出来的最多条数
  baseIndex: baseIndex,
  pannelsLayout: () => {
    return { // 编辑界面的弹出层位置信息
      dimension: {
        top: 72,
        right: 60,
        width: 250,
        height: 316,
        zIndex: baseIndex - 2,
        display: false,
        limit: {
          height: [80]
        },
        box: rootBox
      },
      measure: {
        top: 115,
        right: 60,
        width: 250,
        height: 316,
        limit: {
          height: [80]
        },
        zIndex: baseIndex - 1,
        display: false,
        box: rootBox
      },
      setting: {
        top: 158,
        right: 60,
        width: 250,
        height: 410,
        limit: {
          height: [80]
        },
        zIndex: baseIndex,
        display: false,
        box: rootBox
      },
      datasource: {
        top: 52,
        left: 10,
        width: 250,
        height: 316,
        limit: {
          height: [80]
        },
        zIndex: baseIndex,
        display: true,
        box: rootBox
      },
      search: {
        top: 52,
        left: 10,
        width: 250,
        height: 316,
        limit: {
          height: [80]
        },
        zIndex: baseIndex,
        display: false,
        box: rootBox
      }
    }
  }
}
