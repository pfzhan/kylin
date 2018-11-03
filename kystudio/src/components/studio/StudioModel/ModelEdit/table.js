import { objectClone, sampleGuid } from 'util/index'
import { modelRenderConfig } from './config'
let zIndex = 10
// table 对象
class NTable {
  constructor (options) {
    // this.database = options.database // 数据库名
    // this.tablename = options.tablename // 表名
    this.name = options.table // 全称
    this.columns = objectClone(options.columns) // 所有列
    this.kind = options.kind ? options.kind : options.fact ? modelRenderConfig.tableKind.fact : modelRenderConfig.tableKind.lookup // table 类型
    this.joinInfo = {} // 链接对象
    this.guid = sampleGuid() // identify id
    this.alias = options.alias || options.table // 别名
    this.ST = null
    this.drawSize = Object.assign({}, { // 绘制信息
      left: 100,
      top: 20,
      width: modelRenderConfig.tableBoxWidth,
      height: modelRenderConfig.tableBoxHeight,
      limit: {
        height: [44]
      },
      zIndex: zIndex++,
      sizeChangeCb: () => {
        options.plumbTool.lazyRender(() => {
          options.plumbTool.refreshPlumbInstance()
        })
      }
    }, options.drawSize)
  }
  // 链接关系处理
  addLinkData (fTable, linkColumnF, linkColumnP, type) {
    var pid = this.guid
    // if (this.joinInfo[pid]) {
    //   this.joinInfo[pid].join.type = type
    //   this.joinInfo[pid].join.primary_key.push(...linkColumnP)
    //   this.joinInfo[pid].join.foreign_key.push(...linkColumnF)
    // } else {
    this.joinInfo[pid] = {
      table: {
        guid: this.guid,
        columns: this.columns,
        name: this.name,
        alias: this.alias,
        kind: this.kind
      },
      join: {
        type: type,
        primary_key: [...linkColumnP],
        foreign_key: [...linkColumnF]
      },
      foreignTable: {
        guid: fTable.guid,
        namd: fTable.name
      },
      kind: this.kind
    }
    // }
  }
  getColumnType (columnName) {
    let len = this.columns && this.columns.length || 0
    for (let i = len - 1; i >= 0; i--) {
      if (this.columns[i].name === columnName) {
        return this.columns[i].datatype
      }
    }
  }
  getJoinInfo () {
    return this.joinInfo[this.guid]
  }
  _replaceAlias (alias, fullName) {
    return fullName && fullName.replace(/^([^.]+?)/, alias)
  }
  // 获取符合元数据格式的JoinInfo
  getMetaJoinInfo () {
    let joinInfo = objectClone(this.joinInfo[this.guid])
    let obj = {}
    if (joinInfo && joinInfo.table && joinInfo.join) {
      obj.table = joinInfo.table.name
      obj.alias = joinInfo.table.alias
      let falias = joinInfo.foreignTable.alias
      joinInfo.join.foreign_key.map((x) => {
        return this._replaceAlias(falias, x)
      })
      joinInfo.join.primary_key.map((x) => {
        return this._replaceAlias(joinInfo.table.alias, x)
      })
      obj.join = joinInfo.join
    } else {
      return null
    }
    return obj
  }
  // 获取符合元数据格式的模型坐标位置信息
  getMetaCanvasInfo () {
    return {
      x: this.drawSize.left,
      y: this.drawSize.top,
      width: this.drawSize.width,
      height: this.drawSize.height
    }
  }
  // 改变连接关系
  changeLinkType (pid, type) {
    if (this.joinInfo[pid]) {
      this.joinInfo[pid].join = type
    }
  }
  // 获取所有的连接关系
  get links () {
    let _links = []
    for (var i in this.joinInfo) {
      _links.push(this.joinInfo[i])
    }
    return _links
  }
  // 获取某个主键表相关的连接
  getLinks () {
    return this.joinInfo[this.guid] || {}
  }
  // 可计算列处理
  // 维度处理
  // dimension处理
  // 展示信息处理
  renderLink () {
  }
  setPosition (x, y) {
    this.drawSize.x = x
    this.drawSize.y = y
  }
  setSize (w, h) {
    this.drawSize.width = w
    this.drawSize.height = h
  }
}

export default NTable
