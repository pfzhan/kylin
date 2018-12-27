import { sampleGuid } from 'util/index'

export function renderModelAddTableData (tables, links, factTableName) {
  let guidMap = {}
  let result = []
  tables.forEach((t) => {
    let guid = sampleGuid()
    guidMap[t.name] = guid
    result.push(
      {
        eventID: 6,
        done: false,
        target: 'modelDataSourceTreeScrollBox', // 进入可视区域
        search: '.guide-' + t.name.replace('.', '')
      },
      {
        eventID: 1,
        done: false,
        target: 'modelDataSourceTree', // 点击模型添加左侧树
        search: '.guide-' + t.name.replace('.', '')
      },
      {
        eventID: 11,
        done: false,
        pos: {
          left: t.x,
          top: t.y
        }
      },
      {
        eventID: 2,
        done: false,
        target: 'modelEditAction', // 执行table加载
        val: {
          action: 'addTable',
          data: {tableName: t.name, x: t.x, y: t.y, guid: guid}
        }
      }
    )
  })
  links.forEach((link, i) => {
    let fguid = guidMap[link.ftable]
    let fcolumn = link.fcolumn
    let pguid = guidMap[link.ptable]
    let pcolumn = link.pcolumn
    result.push(
      {
        eventID: 1,
        tip: i === 0 ? '拖动表中列到另外一张表的列来建立关系' : null,
        done: false,
        target: fguid + fcolumn // 连接列
      },
      {
        eventID: 11,
        done: false,
        target: pguid + pcolumn // 连接列
      },
      {
        eventID: 2,
        done: false,
        target: 'modelEditAction', // 执行表关系连接
        val: {
          action: 'link',
          data: {
            fguid: fguid,
            pguid: pguid,
            joinType: link.joinType,
            fColumnName: fcolumn,
            pColumnName: pcolumn
          }
        }
      },
      {
        eventID: 1,
        done: false,
        target: 'saveJoinBtn' // 飞向保存join按钮
      },
      {
        eventID: 2,
        done: false,
        target: 'saveJoinBtn' // 点击保存join按钮
      }
    )
  })
  let factguid = guidMap[factTableName]
  result.push({
    tip: '设置fact表',
    eventID: 1,
    done: false,
    target: factguid, // 飞向设置
    search: '.setting-icon'
  }, {
    eventID: 2,
    done: false,
    target: factguid, // 点击设置
    search: '.setting-icon'
  })
  return result
}
export function renderDimensionData (dimensions) {
  let result = []
  dimensions.forEach((d) => {
    let fullWordName = d.replace('.', '')
    result.push({
      eventID: 6,
      done: false,
      target: 'dimensionScroll', // 进入可视区域
      search: '.guide-' + fullWordName
    }, {
      eventID: 1,
      done: false,
      target: 'batchAddDimensionBox', // 点击dimension 批量弹出按钮按钮
      search: '.guide-' + fullWordName
    }, {
      eventID: 2,
      done: false,
      target: 'batchAddDimensionBox', // 点击dimension 批量弹出按钮按钮
      search: '.guide-' + fullWordName
    })
  })
  return result
}

export function renderMeasuresData (measures) {
  let result = []
  measures.forEach((measure) => {
    result.push(
      {
        eventID: 1,
        done: false,
        target: 'measureAddBtn' // 飞向添加measure的按钮
      },
      {
        eventID: 2,
        done: false,
        target: 'measureAddBtn' // 点击measure的按钮
      },
      // 添加measure
      {
        eventID: 1,
        done: false,
        target: 'measureNameInput' // 飞向添加measure的按钮
      },
      {
        eventID: 3,
        done: false,
        target: 'measureNameInput', // 点击measure的按钮
        val: 'testMeasure' + sampleGuid()
      },
      {
        eventID: 1,
        done: false,
        target: 'measureExpressionSelect' // 飞向表达式选择
      },
      {
        eventID: 31,
        done: false,
        target: 'measureExpressionSelect', // 点击表达式选择
        val: measure.expression
      },
      {
        eventID: 1,
        done: false,
        target: 'measureReturnValSelect' // 飞向 return type
      },
      {
        eventID: 31,
        done: false,
        target: 'measureReturnValSelect', // 选择return type
        val: measure.parameter
      },
      {
        eventID: 1,
        done: false,
        target: 'saveMeasureBtn' // 飞向 return type
      },
      {
        eventID: 2,
        done: false,
        target: 'saveMeasureBtn' // 选择return type
      }
    )
  })
  return result
}

