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
    this.drawSize = Object.assign({}, { // 绘制信息
      left: 100,
      top: 20,
      width: modelRenderConfig.tableBoxWidth,
      height: modelRenderConfig.tableBoxHeight,
      minheight: 44,
      zIndex: zIndex++,
      sizeChangeCb () {
        options.plumbTool.refreshPlumbInstance()
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
