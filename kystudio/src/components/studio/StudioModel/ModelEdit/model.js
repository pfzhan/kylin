import NTable from './table.js'
import store from '../../../../store'
import { jsPlumbTool } from '../../../../util/plumb'
import { parsePath, sampleGuid, indexOfObjWithSomeKey, indexOfObjWithSomeKeys, objectClone } from '../../../../util'
import { modelRenderConfig } from './config'
import ModelTree from './layout'
import $ from 'jquery'
// model 对象
class NModel {
  constructor (options, _mount, _) {
    Object.assign(this, options)
    this.mode = options.uuid ? 'edit' : 'new' // 当前模式
    this.name = options.name
    this.fact_table = options.fact_table
    this.description = options.description
    this.project = options.project
    this.uuid = options.uuid || null
    this.tables = {}
    this.owner = options.owner || ''
    this.canvas = options.canvas // 模型布局坐标
    this.filter_condition = options.filter_condition || null
    this.column_correlations = options.column_correlations || []
    this.computed_columns = options.computed_columns || []
    this.last_modified = options.last_modified || 0
    this.partition_desc = options.partition_desc || {
      partition_date_column: null,
      partition_time_column: null,
      partition_date_start: 0,
      partition_type: 'APPEND'
    }
    this.all_named_columns = options.all_named_columns || []
    this.all_named_columns.forEach((col) => {
      col.guid = sampleGuid()
    })
    this.computed_columns.forEach((col) => {
      col.guid = sampleGuid()
    })
    this.dimensions = this.all_named_columns.filter((x) => {
      if (x.status === 'DIMENSION') {
        let columnNamed = x.column.split('.')
        let k = indexOfObjWithSomeKeys(this.computed_columns, 'tableAlias', columnNamed[0], 'columnName', columnNamed[1])
        if (k >= 0) {
          x.isCC = true
          x.cc = this.computed_columns[k]
        }
        x.datatype = this.getColumnType(x.column)
        return x
      }
    })
    // 用普通列构建的dimension
    this.normalDimensions = this.all_named_columns.filter((x) => {
      let columnNamed = x.column.split('.')
      let i = indexOfObjWithSomeKeys(this.computed_columns, 'columnName', columnNamed[1], 'tableAlias', columnNamed[0])
      if (i < 0 && x.status === 'DIMENSION') {
        return x
      }
    })
    // 用在tableIndex上的列
    this.tableIndexColumns = this.all_named_columns.filter((x) => {
      if (x.status !== 'DIMENSION') {
        return x
      }
    })
    // 可计算列构建的dimension
    this.ccDimensions = this.all_named_columns.filter((x) => {
      let columnNamed = x.column.split('.')
      let i = indexOfObjWithSomeKeys(this.computed_columns, 'columnName', columnNamed[1], 'tableAlias', columnNamed[0])
      if (i >= 0 && x.status === 'DIMENSION') {
        return x
      }
    })
    this.lookups = options.lookups || options.join_tables || []
    this.all_measures = options.simplified_measures || []
    this.project = options.project
    this.maintain_model_type = options.maintain_model_type
    this.management_type = options.management_type
    this.globalDataSource = store.state.datasource.dataSource[this.project] // 全局数据源表数据
    this.datasource = options.simplified_tables || [] // 当前模型使用的数据源表数据
    if (_) {
      this.vm = _
      this._mount = _mount // 挂载对象
      this.$set = _.$set
      this.$delete = _.$delete
      this.plumbTool = jsPlumbTool()
    } else {
      this.$set = function (obj, key, val) {
        obj[key] = val
      }
    }
    if (_mount) {
      this.$set(this._mount, 'computed_columns', this.computed_columns)
      this.$set(this._mount, 'tables', this.tables)
      this.$set(this._mount, 'all_named_columns', this.all_named_columns)
      this.$set(this._mount, 'all_measures', this.all_measures)
      this.$set(this._mount, 'dimensions', this.dimensions)
      this.$set(this._mount, 'zoom', this.canvas && this.canvas.zoom || modelRenderConfig.zoom)
      this.$set(this._mount, 'normalDimensions', this.normalDimensions)
      this.$set(this._mount, 'tableIndexColumns', this.tableIndexColumns)
      this.$set(this._mount, 'ccDimensions', this.ccDimensions)
      this.$set(this._mount, 'maintain_model_type', this.maintain_model_type)
    }
    if (options.renderDom) {
      this.renderDom = this.vm.$el.querySelector(options.renderDom)
      this.plumbTool.init(this.renderDom, this._mount.zoom / 10)
    }
    this.allConnInfo = {}
    this.render()
    this.dimensions = this.dimensions.filter((x) => {
      x.datatype = this.getColumnType(x.column)
      return x
    })
  }
  render () {
    this.renderTable()
    this.vm && this.vm.$nextTick(() => {
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
      initTableOptions.drawSize = this.getTableCoordinate(initTableOptions.alias) // 获取坐标信息
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
        if (this.renderDom) {
          var ftable = this.getTableByAlias(tableObj.join.foreign_key[0].split('.')[0])
          ntable.addLinkData(ftable, tableObj.join.foreign_key, tableObj.join.primary_key, tableObj.join.type)
        }
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
    this.plumbTool.lazyRender(() => {
      for (var guid in this.tables) {
        var curNT = this.tables[guid]
        for (var i in curNT.joinInfo) {
          var primaryGuid = guid
          var foreignGuid = curNT.joinInfo[i].foreignTable.guid
          this.renderLink(primaryGuid, foreignGuid)
        }
      }
    })
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
    return new Promise((resolve, reject) => {
      try {
        let metaData = {
          uuid: this.uuid,
          name: this.name,
          owner: this.owner,
          project: this.project,
          description: this.description
        }
        let factTable = this.getFactTable()
        if (factTable) {
          metaData.fact_table = factTable.name
        } else {
          return reject('000')
        }
        // this.all_measures.push()
        metaData.join_tables = this._generateLookups()
        metaData.all_named_columns = this._generateAllColumns()
        metaData.simplified_measures = this._mount.all_measures
        metaData.computed_columns = objectClone(this.computed_columns)
        metaData.last_modified = this.last_modified
        metaData.filter_condition = this.filter_condition
        metaData.partition_desc = this.partition_desc
        metaData.maintain_model_type = this._mount.maintain_model_type
        metaData.management_type = this.management_type
        metaData.canvas = this._generateTableRectData()
        resolve(metaData)
      } catch (e) {
        reject(e)
      }
    })
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
    return [...this._mount.dimensions, ...this.tableIndexColumns]
  }
  _generateTableRectData () {
    let canvasInfo = {
      coordinate: {}
    }
    for (let t in this.tables) {
      let ntable = this.tables[t]
      canvasInfo.coordinate[ntable.alias] = ntable.getMetaCanvasInfo()
    }
    canvasInfo.zoom = this._mount.zoom
    return canvasInfo
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
      let columns = this.tables[i].columns
      columns && columns.forEach((co) => {
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
        this._delTableRelated(guid)
        this.$delete(this.tables, guid)
        return resolve()
      } else {
        // 有连接的情况下
        return reject()
      }
    })
  }
  _delTableRelated (guid) {
    let ntable = this.getTableByGuid(guid)
    if (ntable) {
      let alias = ntable.alias
      // 删除对应的 dimension
      this._delDimensionByAlias(alias)
      // 删除对应的 measure
      this._delMeasureByAlias(alias)
      // 删除对应的 cc
      this._delCCByAlias(alias)
    }
  }
  getTable (key, val) {
    for (var i in this.tables) {
      if (this.tables[i][key] === val) {
        return this.tables[i]
      }
    }
  }
  getTables (key, val) {
    let result = []
    for (var i in this.tables) {
      if (this.tables[i][key] === val) {
        result.push(this.tables[i])
      }
    }
    return result
  }
  getTableColumns () {
    let result = []
    for (var i in this.tables) {
      let columns = this.tables[i].columns
      columns && columns.forEach((col) => {
        col.guid = i // 永久指纹
        col.table_alias = this.tables[i].alias // 临时
        col.full_colname = col.table_alias + '.' + col.name
        result.push(col)
      })
    }
    return result
  }
  getTableCoordinate (alias) {
    if (this.canvas) {
      for (let i in this.canvas.coordinate) {
        if (i === alias) {
          let _info = this.canvas.coordinate[i]
          return {
            left: _info.x,
            top: _info.y,
            width: _info.width,
            height: _info.height
          }
        }
      }
    }
  }
  changeTableType (t) {
    t.kind = t.kind === modelRenderConfig.tableKind.fact ? modelRenderConfig.tableKind.lookup : modelRenderConfig.tableKind.fact
    this.setUniqueAlias(t)
    // 更新facttable的名字
    // 删除原来facttable上的cc
  }
  _checkSameAlias (guid, newAlias) {
    var hasAlias = 0
    Object.values(this.tables).forEach(function (table) {
      if (table.guid !== guid) {
        if (table.alias.toUpperCase() === newAlias.toUpperCase()) {
          hasAlias++
        }
      }
    })
    return hasAlias
  }
  _createUniqueName (guid, alias) {
    if (alias && guid) {
      var sameCount = this._checkSameAlias(guid, alias)
      var finalAlias = alias.toUpperCase().replace(/[^a-zA-Z_0-9]/g, '')
      if (sameCount === 0) {
        return finalAlias
      } else {
        while (this._checkSameAlias(guid, finalAlias + '_' + sameCount)) {
          sameCount++
        }
        return finalAlias + '_' + sameCount
      }
    }
  }
  setUniqueAlias (table) {
    // fact 情况的特殊处理
    if (table.kind === modelRenderConfig.tableKind.fact) {
      let sameTable = this.getTables('name', table.name)
      for (let i = 0; i < sameTable.length; i++) {
        const t = sameTable[i]
        if (t.guid !== table.guid) {
          t.alias = table.alias
          break
        }
      }
      table.alias = table.name.split('.')[1]
    } else {
      var uniqueName = this._createUniqueName(table.guid, table.alias)
      this.$set(table, 'alias', uniqueName)
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
      } else {
        this.tables[table.guid] = table
      }
      if (this.renderDom) {
        this.plumbTool.draggable([table.guid])
      }
      this.setUniqueAlias(table)
      return table
    }
    return this.tables[options.alias]
  }
  _getTableOriginInfo (tableFullName) {
    let tableNamed = tableFullName.split('.')
    if (this.datasource) {
      let i = indexOfObjWithSomeKey(this.datasource, 'table', tableFullName)
      if (i >= 0) {
        return this.datasource[i]
      }
    }
    if (this.globalDataSource) {
      let i = indexOfObjWithSomeKeys(this.globalDataSource, 'database', tableNamed[0], 'name', tableNamed[1])
      if (i >= 0) {
        let globalTableInfo = this.globalDataSource[i]
        globalTableInfo.table = globalTableInfo.database + '.' + globalTableInfo.name
        this.datasource.push(globalTableInfo)
        return this.globalDataSource[i]
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
  getColumnType (fullName) {
    let named = fullName.split('.')
    if (named && named.length) {
      let alias = named[0]
      let tableName = named[1]
      let ntable = this.getTableByAlias(alias)
      return ntable && ntable.getColumnType(tableName)
    }
  }
  getCCObj (tableAlias, column) {
    let i = indexOfObjWithSomeKeys(this.computed_columns, 'columnName', column, 'tableAlias', tableAlias)
    if (i >= 0) {
      return this.computed_columns[i]
    }
  }
  getFactTable () {
    for (var i in this.tables) {
      if (this.tables[i].kind === modelRenderConfig.tableKind.fact) {
        return this.tables[i]
      }
    }
  }
  // 添加维度
  addDimension (dimension) {
    return new Promise((resolve, reject) => {
      if (indexOfObjWithSomeKey(this.dimensions, 'name', dimension.name) <= 0) {
        dimension.guid = sampleGuid()
        this._mount.dimensions.push(dimension)
        resolve(dimension)
      } else {
        reject()
      }
    })
  }
  // 添加度量
  editDimension (dimension, i) {
    return new Promise((resolve, reject) => {
      let index = indexOfObjWithSomeKey(this._mount.dimensions, 'guid', dimension.guid)
      Object.assign(this._mount.dimensions[index], dimension)
      resolve()
    })
  }
  delDimension (i) {
    this._mount.dimensions.splice(i, 1)
  }
  _delDimensionByAlias (alias) {
    let dimensions = this._mount.dimensions.filter((item) => {
      return item.column && item.column.split('.')[0] !== alias
    })
    this._mount.dimensions.splice(0, this._mount.dimensions.length)
    this._mount.dimensions.push(...dimensions)
  }
  delMeasure (i) {
    this._mount.all_measures.splice(i, 1)
  }

  // measure parameterValue 临时结构
  _delMeasureByAlias (alias) {
    let measures = this._mount.all_measures.filter((item) => {
      if (item.parameter_value[0] && item.parameter_value[0].type === 'column' && item.parameter_value[0].value.split('.')[0] !== alias) {
        if (item.converted_columns && item.converted_columns.length > 0) {
          const finalConvertedColumns = item.converted_columns.filter((column) => {
            return column.type === 'column' && column.value.split('.')[0] !== alias
          })
          item.converted_columns.splice(0, item.converted_columns.length)
          item.converted_columns.push(...finalConvertedColumns)
        }
        return item
      }
    })
    this._mount.all_measures.splice(0, this._mount.all_measures.length)
    this._mount.all_measures.push(...measures)
  }
  // 添加度量
  addMeasure (measureObj) {
    return new Promise((resolve, reject) => {
      if (indexOfObjWithSomeKey(this._mount.all_measures, 'name', measureObj.name) <= 0) {
        measureObj.guid = sampleGuid()
        this._mount.all_measures.push(measureObj)
        resolve(measureObj)
      } else {
        reject()
      }
    })
  }
  // 编辑度量
  editMeasure (measureObj) {
    return new Promise((resolve, reject) => {
      let index = indexOfObjWithSomeKey(this._mount.all_measures, 'guid', measureObj.guid)
      Object.assign(this._mount.all_measures[index], measureObj)
      resolve()
    })
  }
  // 检查是否有同名
  _checkSameCCName (name) {
    return indexOfObjWithSomeKey(this._mount.computed_columns, 'name', name) < 0
  }
  generateCCMeta (ccObj) {
    let factTable = this.getFactTable()
    if (factTable) {
      let ccBase = {
        tableIdentity: factTable.alias,
        tableAlias: factTable.alias,
        guid: sampleGuid()
      }
      Object.assign(ccBase, ccObj)
      return ccBase
    }
  }
  // 添加CC
  addCC (ccObj) {
    return new Promise((resolve, reject) => {
      if (this._checkSameCCName) {
        let ccMeta = this.generateCCMeta(ccObj)
        this._mount.computed_columns.push(ccMeta)
        resolve(ccMeta)
      } else {
        reject()
      }
    })
  }
  // 编辑CC
  editCC (ccObj) {
    return new Promise((resolve, reject) => {
      let hasEdit = false
      this._mount.computed_columns.forEach((c) => {
        if (c.guid === ccObj.guid) {
          Object.assign(c, ccObj)
          hasEdit = true
          resolve(c)
        }
      })
      if (!hasEdit) {
        reject()
      }
    })
  }
  delCC (ccObj) {
    return new Promise((resolve) => {
      for (let i = 0; i < this._mount.computed_columns.length; i++) {
        const c = this._mount.computed_columns[i]
        if (c.guid === ccObj.guid) {
          this._mount.computed_columns.splice(i, 1)
          resolve(c)
          break
        }
      }
    })
  }
  _delCCByAlias () {
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
