import NTable from './table.js'
import store from '../../../../store'
import { jsPlumbTool } from '../../../../util/business'
import { parsePath, isEmptyObject } from '../../../../util'
import { modelRenderConfig } from './config'
import ModelTree from './layout'
import $ from 'jquery'
// model 对象
class NModel {
  constructor (options, _mount, _) {
    this.mode = options.uuid ? 'edit' : 'new' // 当前模式
    this.name = options.name
    this.fact_table = options.fact_table
    this.uuid = options.uuid || null
    this.tables = {}
    this.filter_condition = options.filter_condition || null
    this.column_correlations = options.column_correlations || []
    this.computed_columns = options.computed_columns || []
    this.last_modified = options.last_modified || 0
    this.partition_desc = options.partition_desc || {}
    this.all_named_columns = options.all_named_columns || []
    this.lookups = options.lookups || []
    this.dimensions = options.dimensions || []
    this.all_measures = options.all_measures || []
    this.project = options.project
    this.datasource = store.state.datasource.dataSource[this.project]
    this.vm = _
    this._mount = _mount // 挂载对象
    this.$set = _.$set
    this.$delete = _.$delete
    this.plumbTool = jsPlumbTool()
    this.$set(this._mount, 'tables', this.tables)
    this.$set(this._mount, 'all_measures', this.all_measures)
    this.$set(this._mount, 'dimensions', this.dimensions)
    this.$set(this._mount, 'zoom', modelRenderConfig.zoom)
    this.renderDom = this.vm.$el.querySelector(options.renderDom)
    this.plumbTool.init(this.renderDom, this._mount.zoom / 10)
    this.allConnInfo = {}
    this.render()
  }
  render () {
    this.renderTable()
    this.vm.$nextTick(() => {
      this.renderLinks()
      this.renderPosition()
      setTimeout(() => {
        this.renderLabels()
      }, 1)
    })
    // renderDimension
    // renderMeasure
  }
  renderTable () {
    if (this.mode === 'edit') {
      this.addTable({
        alias: this.fact_table.split('.')[1],
        columns: this._getTableColumns(this.fact_table),
        kind: 'FACT',
        table: this.fact_table
      })
      this.lookups.forEach((tableObj) => {
        let ntable = this.addTable({
          alias: tableObj.alias,
          columns: this._getTableColumns(tableObj.table),
          kind: tableObj.kind,
          table: tableObj.table
        })
        tableObj.join.foreign_key.forEach((fkey, i) => {
          var ptable = this.getTableByAlias(fkey.split('.')[0])
          ntable.addLinkData(ptable, fkey, tableObj.join.primary_key[i], tableObj.join.type)
        })
      })
    }
  }
  renderPosition () {
    const layers = this.autoCalcLayer()
    const baseL = modelRenderConfig.baseLeft
    const baseT = modelRenderConfig.baseTop
    const centerL = $(this.renderDom).width() / 2 - modelRenderConfig.tableBoxWidth / 2
    const moveL = layers[0].X - centerL
    for (let k = 0; k < layers.length; k++) {
      var currentTable = this.getTableByGuid(layers[k].guid)
      currentTable.drawSize.left = baseL - moveL + layers[k].X
      currentTable.drawSize.top = baseT + layers[k].Y
      currentTable.drawSize.width = modelRenderConfig.tableBoxWidth
      currentTable.drawSize.height = modelRenderConfig.tableBoxHeight
    }
    this.vm.$nextTick(() => {
      this.plumbTool.refreshPlumbInstance(this.plumbInstance)
    })
  }
  renderLinks () {
    for (var guid in this.tables) {
      var curNT = this.tables[guid]
      for (var i in curNT.joinInfo) {
        var primaryGuid = guid
        var foreignGuid = curNT.joinInfo[i].foreignTable.guid
        var conn = this.renderLink(primaryGuid, foreignGuid)
        this.allConnInfo[primaryGuid + '$' + foreignGuid] = conn
      }
    }
  }
  renderLink (pid, fid) {
    this.addPlumbPoints(pid, '', '', true)
    this.addPlumbPoints(fid, '', '', true)
    if (this.allConnInfo[pid + '$' + fid]) {
      return this.allConnInfo[pid + '$' + fid]
    }
    var conn = this.plumbTool.connect(pid, fid, () => {
      this.connClick(pid, fid)
    }, {})
    this.setOverLayLabel(conn)
    this.plumbTool.refreshPlumbInstance()
    return conn
  }
  renderLabels () {
    for (var i in this.allConnInfo) {
      this.setOverLayLabel(this.allConnInfo[i])
    }
  }
  search (keywords) {
    var stables = this.searchTable(keywords)
    var smeasures = this.searchMeasure(keywords)
    var sdimensions = this.searchDimension(keywords)
    var sjoins = this.searchJoin(keywords)
    var scolumns = this.searchColumn(keywords)
    // console.log(stables, smeasures, sdimensions, sjoins)
    return [].concat(stables, smeasures, sdimensions, sjoins, scolumns)
  }
  // search
  searchTable (keywords) {
    return this.mixResult(this.tables, 'table', 'name', keywords)
  }
  searchMeasure (keywords) {
    return this.mixResult(this.all_measures, 'measure', 'name', keywords)
  }
  searchDimension (keywords) {
    return this.mixResult(this.dimensions, 'dimension', 'table', keywords)
  }
  searchJoin (keywords) {
    return this.mixResult(this.lookups, 'join', 'table', keywords)
  }
  searchColumn (keywords) {
    return this.mixResult(this.all_named_columns, 'column', 'name', keywords)
  }
  searchRule (content, keywords) {
    var reg = new RegExp(keywords, 'i')
    return reg.test(content)
  }
  mixResult (data, kind, key, searchVal) {
    let result = []
    let actionsConfig = modelRenderConfig.searchAction[kind]
    if (Object.prototype.toString.call(data) === '[object Object]') {
      for (var i in data) {
        actionsConfig.forEach((a) => {
          if (this.searchRule(data[i][key], searchVal) && result.length < modelRenderConfig.searchCountLimit) {
            result.push({name: data[i][key], kind: kind, action: a.action, i18n: a.i18n})
          }
        })
      }
    } else if (Object.prototype.toString.call(data) === '[object Array]') {
      data && data.forEach((t) => {
        actionsConfig.forEach((a) => {
          if (this.searchRule(t[key], searchVal) && result.length < modelRenderConfig.searchCountLimit) {
            result.push({name: t[key], kind: kind, action: a.action, i18n: a.i18n})
          }
        })
      })
    }
    return result
  }
  delTable (guid) {
    if (this.tables[guid] && isEmptyObject(this.tables[guid].joinInfo) && this.tables[guid].kind !== modelRenderConfig.tableKind.fact) {
      this.$delete(this.tables, guid)
    } else {
      // 有连接的情况下
    }
  }
  getTable (key, val) {
    for (var i in this.tables) {
      if (this.tables[i][key] === val) {
        return this.tables[i]
      }
    }
  }
  // 设置当前最上层的table（zindex）
  setIndexTop (data, t, path) {
    let maxZindex = -1
    var pathObj = parsePath(path)
    data.forEach((x) => {
      if (pathObj(x).zIndex > maxZindex) {
        maxZindex = pathObj(x).zIndex
      }
      if (pathObj(x).zIndex > pathObj(t).zIndex) {
        pathObj(x).zIndex--
      }
    })
    pathObj(t).zIndex = maxZindex
  }
  addZoom () {
    var nextZoom = this._mount.zoom + 1 > 10 ? 10 : this._mount.zoom += 1
    this.plumbTool.setZoom(nextZoom / 10)
  }
  reduceZoom () {
    var nextZoom = this._mount.zoom - 1 < 4 ? 4 : this._mount.zoom -= 1
    this.plumbTool.setZoom(nextZoom / 10)
  }
  bindConnClickEvent (cb) {
    this.connClick = (pid, fid) => {
      var pntable = this.getTableByGuid(pid)
      var fntable = this.getTableByGuid(fid)
      cb && cb(pntable, fntable)
    }
  }
  moveModelPosition (x, y) {
    if (x !== +x || y !== +y) {
      return
    }
    for (var i in this.tables) {
      var curTable = this.tables[i]
      curTable.drawSize.left += x
      curTable.drawSize.top += y
    }
    this.vm.$nextTick(() => {
      this.plumbTool.refreshPlumbInstance()
    })
  }
  addTable (options) {
    if (!this.tables[options.alias]) {
      options.columns = this._getTableColumns(options.table)
      options.plumbTool = this.plumbTool
      let table = new NTable(options)
      // this.tables[options.alias] = table
      if (this.vm) {
        this.vm.$set(this._mount.tables, table.guid, table)
      }
      this.plumbTool.draggable([table.guid])
      return table
    }
    return this.tables[options.alias]
  }
  _getTableColumns (tableFullName) {
    if (this.datasource) {
      for (var i = this.datasource.length - 1; i >= 0; i--) {
        if (this.datasource[i].database + '.' + this.datasource[i].name === tableFullName) {
          return this.datasource[i].columns
        }
      }
    }
    return []// 需要报错
  }
  getTableByGuid (guid) {
    for (var i in this.tables) {
      if (i === guid) {
        return this.tables[i]
      }
    }
  }
  getTableByAlias (alias) {
    for (var i in this.tables) {
      if (this.tables[i].alias === alias) {
        return this.tables[i]
      }
    }
  }
  getFactTable () {
    for (var i in this.tables) {
      if (this.tables[i].kind === modelRenderConfig.tableKind.fact) {
        return this.tables[i]
      }
    }
  }
  autoCalcLayer (root, result, deep) {
    var factTable = this.getFactTable()
    if (!factTable) {
      return
    }
    const rootGuid = factTable.guid
    const tree = new ModelTree({rootGuid: rootGuid, showLinkCons: this.allConnInfo})
    tree.positionTree()
    return tree.nodeDB.db
  }
  // 归整all_named_columns信息
  _collectColumns () {
    let columnsCollect = {}
    this.all_named_columns.forEach((x) => {
      let fullNameSplit = x.column.split('.')
      let alias = fullNameSplit[0]
      let columnName = fullNameSplit[1]
      let columnInfo = {id: x.id, name: x.name, column: columnName, fullName: x.column}
      if (columnsCollect[alias]) {
        columnsCollect[alias].push(columnInfo)
      } else {
        columnsCollect[alias] = [columnInfo]
      }
    })
  }
  renderColumnType () {
    let collectColumns = this._collectColumns()
    for (let c in collectColumns) {
      var nt = this.tables[c]
      nt.columns.forEach((x) => {
        collectColumns[c].forEach((y) => {
          if (x.name === y.column) {
            x.id = y.id
            x.name = y.name
            x.checked = true
            x.column = y.fullName
          }
        })
      })
    }
  }
  addPlumbPoints (guid, columnName, columnType) {
    var anchor = modelRenderConfig.jsPlumbAnchor
    var scope = 'showlink'
    var endPointConfig = Object.assign({}, this.plumbTool.endpointConfig, {
      scope: scope,
      parameters: {
        data: {
          guid: guid,
          column: {
            columnName: columnName,
            columnType: columnType
          }}
      },
      uuid: guid + columnName
    })
    this.plumbTool.addEndpoint(guid, {anchor: anchor}, endPointConfig)
  }
  setOverLayLabel (conn) {
    var fid = conn.sourceId
    var pid = conn.targetId
    var labelObj = conn.getOverlay(pid + (fid + 'label'))
    var joinInfo = this.tables[pid].getJoinInfo()
    if (!joinInfo) {
      return
    }
    var joinType = joinInfo.join.type
    var labelCanvas = $(labelObj.canvas)
    labelCanvas.addClass(joinType === modelRenderConfig.joinKind.left ? 'label-left' : 'label-inner')
    labelObj.setLabel(joinType)
  }
}

export default NModel
