import { sampleGuid } from 'util/index'
import { highGuideSpeed, normalGuideSpeed } from './config'
function switchGuideSpeed (i) {
  return i === 0 ? normalGuideSpeed : highGuideSpeed
}
export function batchRequest (sqls) {
  let result = []
  sqls.forEach((s) => {
    result.push({
      eventID: 21,
      done: false,
      target: 'queryTriggerBtn', // 在编辑器中输入需要查询的SQL
      val: {
        action: 'requestSql',
        data: s
      },
      timer: 100
    })
  })
  return result
}
export function renderModelAddTableData (tables, links, factTableName) {
  let guidMap = {}
  let result = []
  tables.forEach((t, i) => {
    let guid = sampleGuid()
    guidMap[t.name] = guid
    let timer = switchGuideSpeed(i)
    result.push(
      {
        eventID: 6,
        done: false,
        target: 'modelDataSourceTreeScrollBox', // 进入可视区域
        search: '.guide-' + t.name.replace('.', '').toLowerCase(),
        timer: timer
      },
      {
        eventID: 1,
        done: false,
        target: 'modelDataSourceTree', // 点击模型添加左侧树
        search: '.guide-' + t.name.replace('.', '').toLowerCase(),
        timer: timer
      },
      {
        eventID: 11,
        done: false,
        pos: {
          left: t.x,
          top: t.y
        },
        timer: timer
      },
      {
        eventID: 2,
        done: false,
        target: 'modelEditAction', // 执行table加载
        val: {
          action: 'addTable',
          data: {tableName: t.name, x: t.x, y: t.y, guid: guid}
        },
        timer: timer
      }
    )
  })
  result.push({
    eventID: 2,
    done: false,
    target: 'moveGuidePanelBtn', // 移动guide面板
    val: {
      left: 22,
      top: 10
    }
  })
  links.forEach((link, i) => {
    let fguid = guidMap[link.ftable]
    let fcolumn = link.fcolumn
    let pguid = guidMap[link.ptable]
    let pcolumn = link.pcolumn
    let timer = switchGuideSpeed(i)
    result.push(
      {
        eventID: 1,
        done: false,
        offsetX: -100,
        target: fguid + fcolumn, // 连接列
        timer: timer
      },
      {
        eventID: 11,
        offsetX: -100,
        done: false,
        target: pguid + pcolumn, // 连接列
        timer: timer
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
        },
        timer: timer
      },
      {
        eventID: 1,
        done: false,
        target: 'saveJoinBtn', // 飞向保存join按钮
        timer: timer
      },
      {
        eventID: 2,
        done: false,
        target: 'saveJoinBtn', // 点击保存join按钮
        timer: timer
      }
    )
  })
  let factguid = guidMap[factTableName]
  result.push({
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
  result.push({
    eventID: 2,
    done: false,
    target: 'moveGuidePanelBtn', // 移动guide面板
    val: {
      right: 10,
      top: 10
    }
  })
  dimensions.forEach((d, i) => {
    let fullWordName = d.replace('.', '')
    let timer = switchGuideSpeed(i)
    result.push({
      eventID: 6,
      done: false,
      target: 'dimensionScroll', // 进入可视区域
      search: '.guide-' + fullWordName,
      timer: timer
    }, {
      eventID: 1,
      done: false,
      target: 'batchAddDimensionBox', // 点击dimension 批量弹出按钮按钮
      search: '.guide-' + fullWordName,
      offsetX: -910,
      timer: timer
    }, {
      eventID: 2,
      done: false,
      target: 'batchAddDimensionBox', // 点击dimension 批量弹出按钮按钮
      search: '.guide-' + fullWordName,
      offsetX: -910,
      timer: timer
    })
  })
  return result
}

export function renderLoadHiveTables (target, tables) {
  let result = []
  for (let i in tables) {
    let dbname = i.toLowerCase()
    result.push({
      eventID: 1,
      done: false,
      target: target, // 飞向指定的database
      search: '.guide-' + dbname,
      offsetX: -370
    }, {
      eventID: 2,
      done: false,
      target: target, // 点击指定的database
      search: '.guide-' + dbname,
      offsetX: -370
    })
    tables[i].forEach((t, i) => {
      t = t.toLowerCase()
      let timer = switchGuideSpeed(i)
      result.push({
        eventID: 6,
        done: false,
        target: 'tableScroll', // 进入可视区域
        search: '.guide-' + t,
        timer: timer
      }, {
        eventID: 1,
        done: false,
        target: target, // 飞向指定的列
        search: '.guide-' + t,
        offsetX: -370,
        timer: timer
      }, {
        eventID: 2,
        done: false,
        target: target, // 点击指定的列
        search: '.guide-' + t,
        offsetX: -370,
        timer: timer
      })
    })
  }
  return result
}

export function renderMeasuresData (measures) {
  let result = []
  measures.forEach((measure, i) => {
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
        val: 'testMeasure' + i
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

