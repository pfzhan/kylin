import NTable from './table.js'
import store from '../../../../store'
import { jsPlumbTool } from '../../../../util/plumb'
import { parsePath, objectArraySort } from '../../../../util'
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
    this.canvas = options.canvas // 模型布局坐标
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
    this.$set(this._mount, 'computed_columns', this.computed_columns)
    this.$set(this._mount, 'tables', this.tables)
    this.$set(this._mount, 'all_named_columns', this.all_named_columns)
    this.$set(this._mount, 'all_measures', this.all_measures)
    this.$set(this._mount, 'dimensions', this.dimensions)
    this.$set(this._mount, 'zoom', this.canvas && this.canvas.zoom || modelRenderConfig.zoom)
    this.renderDom = this.vm.$el.querySelector(options.renderDom)
    this.plumbTool.init(this.renderDom, this._mount.zoom / 10)
    this.allConnInfo = {}
    this.render()
  }
  render () {
    this.renderTable()
    this.vm.$nextTick(() => {
      this.renderLinks()
      // 如果没有布局信息，就走自动布局程序
      if (!this.canvas) {
        this.renderPosition()
      }
      setTimeout(() => {
        this.renderLabels()
      }, 1)
    })
    // renderDimension
    // renderMeasure
  }
  renderTable () {
    if (this.mode === 'edit') {
      let factTableInfo = this._getTableOriginInfo(this.fact_table)
      let initTableOptions = {
        alias: this.fact_table.split('.')[1],
        columns: factTableInfo.columns,
        fact: factTableInfo.fact,
        kind: 'FACT',
        table: this.fact_table
      }
      initTableOptions.drawSize = this.getTableCoordinate(this.fact_table) // 获取坐标信息
      this.addTable(initTableOptions)
      this.lookups.forEach((tableObj) => {
        let tableInfo = this._getTableOriginInfo(tableObj.table)
        let initTableInfo = {
          alias: tableObj.alias,
          columns: tableInfo.columns,
          fact: tableInfo.fact,
          kind: tableObj.kind,
          table: tableObj.table
        }
        initTableInfo.drawSize = this.getTableCoordinate(tableObj.alias) // 获取坐标信息
        let ntable = this.addTable(initTableInfo)
        // 获取外键表对象
        var ftable = this.getTableByAlias(tableObj.join.foreign_key[0].split('.')[0])
        ntable.addLinkData(ftable, tableObj.join.foreign_key, tableObj.join.primary_key, tableObj.join.type)
      })
    }
  }
  renderPosition () {
    const layers = this.autoCalcLayer()
    if (layers && layers.length > 0) {
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
  }
  renderLinks () {
    for (var guid in this.tables) {
      var curNT = this.tables[guid]
      for (var i in curNT.joinInfo) {
        var primaryGuid = guid
        var foreignGuid = curNT.joinInfo[i].foreignTable.guid
        this.renderLink(primaryGuid, foreignGuid)
      }
    }
  }
  renderLink (pid, fid) {
    this.addPlumbPoints(pid, '', '', true)
    this.addPlumbPoints(fid, '', '', true)
    var hasConn = this.allConnInfo[pid + '$' + fid]
    if (hasConn) {
      let joinInfo = this.tables[pid].getLinks()
      var primaryKeys = joinInfo && joinInfo.join && joinInfo.join.primary_key
      // 如果渲染的时候发现连接关系都没有了，直接删除
      if (!primaryKeys || primaryKeys && primaryKeys.length === 1 && primaryKeys[0] === '') {
        this.removeRenderLink(hasConn)
        return null
      }
      return hasConn
    }
    var conn = this.plumbTool.connect(pid, fid, () => {
      this.connClick(pid, fid)
    }, {})
    this.setOverLayLabel(conn)
    this.plumbTool.refreshPlumbInstance()
    this.allConnInfo[pid + '$' + fid] = conn
    return conn
  }
  // 生成供后台使用的数据结构
  generateMetadata () {
    let metaData = {
      uuid: this.uuid,
      name: this.name,
      fact_table: this.fact_table,
      lookups: this._generateLookups(),
      all_named_columns: this._generateAllColumns(),
      all_measures: this.all_measures,
      computed_columns: this.computed_columns,
      last_modified: this.last_modified,
      filter_condition: this.filter_condition,
      partition_desc: this.partition_desc
    }
    return metaData
  }
  _generateLookups () {
    let result = []
    for (let key in this.tables) {
      let t = this.tables[key]
      if (t.alias !== this.fact_table) {
        var joinInfo = t.getMetaJoinInfo()
        if (joinInfo) {
          result.push(joinInfo)
        }
      }
    }
    return result
  }
  _generateAllColumns () {
    return this.all_named_columns
  }
  // end
  // 判断是否table有关联的链接
  isConnectedTable (guid) {
    var reg = new RegExp('^' + guid + '\\$|\\$' + guid + '$')
    for (let i in this.allConnInfo) {
      if (reg.test(i)) {
        return true
      }
    }
  }
  // 删除conn相关的主键的连接信息
  removeRenderLink (conn) {
    var fid = conn.sourceId
    var pid = conn.targetId
    delete this.allConnInfo[pid + '$' + fid]
    this.plumbTool.deleteConnect(conn)
    this.tables[pid].joinInfo = {}
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
    return this.mixResult(Object.values(this.tables), 'table', 'name', keywords)
  }
  searchMeasure (keywords) {
    return this.mixResult(this.all_measures, 'measure', 'name', keywords)
  }
  searchDimension (keywords) {
    var dimensionColumns = []
    this.dimensions.forEach((x) => {
      x.columns.forEach((c) => {
        dimensionColumns.push({name: c, table: x.table})
      })
    })
    return this.mixResult(dimensionColumns, 'dimension', 'name', keywords)
  }
  searchJoin (keywords) {
    return this.mixResult(this.lookups, 'join', 'table', keywords)
  }
  searchColumn (keywords) {
    var columns = []
    for (var i in this.tables) {
      this.tables[i].columns.forEach((co) => {
        co.alias = this.tables[i].alias + '.' + co.name
        co.guid = this.tables[i].guid
      })
      columns = columns.concat(this.tables[i].columns)
    }
    return this.mixResult(columns, 'column', 'alias', keywords)
  }
  renderSearchResult (t, key, kind, a) {
    let item = {name: t[key], kind: kind, action: a.action, i18n: a.i18n, more: t}
    if (kind === 'table' && a.action === 'tableeditjoin') {
      let joinInfo = t.joinInfo[t.guid]
      if (joinInfo) {
        item.extraInfo = ' <span class="jtk-overlay">' + joinInfo.join.type + '</span> ' + joinInfo.foreignTable.name
      } else {
        return ''
      }
    }
    return item
  }
  searchRule (content, keywords) {
    var reg = new RegExp(keywords, 'i')
    return reg.test(content)
  }
  mixResult (data, kind, key, searchVal) {
    let result = []
    let actionsConfig = modelRenderConfig.searchAction[kind]
    data && data.forEach((t) => {
      actionsConfig.forEach((a) => {
        if (this.searchRule(t[key], searchVal) && result.length < modelRenderConfig.searchCountLimit) {
          let item = this.renderSearchResult(t, key, kind, a)
          if (item) {
            result.push(item)
          }
        }
      })
    })
    return result
  }
  delTable (guid) {
    return new Promise((resolve, reject) => {
      if (!this.isConnectedTable(guid)) {
        this.$delete(this.tables, guid)
        resolve()
      } else {
        // 有连接的情况下
        reject()
      }
    })
  }
  getTable (key, val) {
    for (var i in this.tables) {
      if (this.tables[i][key] === val) {
        return this.tables[i]
      }
    }
  }
  getTableColumns () {
    let result = []
    for (var i in this.tables) {
      Array.prototype.push.apply(result, this.tables[i].columns)
    }
    return result
  }
  getTableCoordinate (alias) {
    if (this.canvas) {
      for (let i in this.canvas.coordinate) {
        if (i === alias) {
          let _info = this.canvas.coordinate[i]
          return {
            left: _info.x_position,
            top: _info.y_position,
            width: _info.width,
            height: _info.height
          }
        }
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
      let tableInfo = this._getTableOriginInfo(options.table)
      options.columns = tableInfo.columns
      options.plumbTool = this.plumbTool
      options.fact = tableInfo.fact
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
  _getTableOriginInfo (tableFullName) {
    if (this.datasource) {
      for (var i = this.datasource.length - 1; i >= 0; i--) {
        if (this.datasource[i].database + '.' + this.datasource[i].name === tableFullName) {
          return this.datasource[i]
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
  getAllColumnsMaxId () {
    let sortedColumns = objectArraySort(this.all_named_columns, false, 'id')
    let maxId = 0
    if (sortedColumns && sortedColumns.length) {
      maxId = sortedColumns[0].id
    }
    return maxId
  }
  // 添加维度
  addDimension (dimObj) {
    dimObj.is_dimension = true
    let maxId = this.getAllColumnsMaxId()
    if (!dimObj.id) {
      dimObj.id = maxId + 1
    }
    this.all_named_columns.push(dimObj)
  }
  // 添加度量
  editDimsnion (dimObj) {
    dimObj.is_dimension = true
    this.all_named_columns.forEach((d) => {
      if (dimObj.id === d.id) {
        d = dimObj
      }
    })
  }
  // 添加度量
  addMeasure (measureObj) {
    this.all_measures.push(measureObj)
  }
  // 编辑度量
  editMeasure (measureObj) {
    this.all_measures.forEach((m) => {
      if (m.name === measureObj.name) {
        m = measureObj
      }
    })
  }
  // 添加CC
  addCC (ccObj) {
    let ccBase = {
      tableIdentity: this.fact_table,
      tableAlias: this.fact_table.split('.')[1]
    }
    Object.assign(ccBase, ccObj)
    this.computed_columns.push(ccBase)
  }
  // 编辑CC
  editCC (ccObj) {
    this.computed_columns.forEach((c) => {
      if (c.columnName === ccObj.name) {
        Object.assign(c, ccObj)
      }
    })
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
