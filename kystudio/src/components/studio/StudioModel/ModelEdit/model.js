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
    if (!options) {
      console.log('model init failed')
      return null
    }
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
        return x
      }
    })
    // 用在tableIndex上的列
    this.tableIndexColumns = this.all_named_columns.filter((x) => {
      if (x.status !== 'DIMENSION') {
        return x
      }
    })
    this.lookups = options.lookups || options.join_tables || []
    this.all_measures = options.simplified_measures || []
    this.project = options.project
    this.maintain_model_type = options.maintain_model_type
    this.management_type = options.management_type || 'TABLE_ORIENTED'
    this.globalDataSource = store.state.datasource.dataSource // 全局数据源表数据
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
      this.$set(this._mount, 'zoomXSpace', 0)
      this.$set(this._mount, 'zoomYSpace', 0)
      this.$set(this._mount, 'tableIndexColumns', this.tableIndexColumns)
      this.$set(this._mount, 'maintain_model_type', this.maintain_model_type)
      this.$set(this._mount, 'management_type', this.management_type)
    }
    if (options.renderDom) {
      this.renderDom = this.vm.$el.querySelector(options.renderDom)
      this.plumbTool.init(this.renderDom, this._mount.zoom / 10)
    }
    this.allConnInfo = {}
    this.render()
    this.getZoomSpace()
  }
  // 初始化数据和渲染数据
  render () {
    this._renderTable()
    this.vm && this.vm.$nextTick(() => {
      this._renderLinks()
      // 如果没有布局信息，就走自动布局程序
      if (!this.canvas) {
        this.renderPosition()
      }
      setTimeout(() => {
        this._renderLabels()
      }, 1)
    })
    // renderDimension
    this.dimensions = this.dimensions.filter((x) => {
      x.datatype = this.getColumnType(x.column)
      let alias = x.column.split('.')[0]
      let guid = this._cacheAliasAndGuid(alias)
      x.table_guid = guid
      return x
    })
    // renderMeasure
    this.all_measures.forEach((x) => {
      const alias = x.parameter_value[0].value.split('.')[0]
      const nTable = this.getTableByAlias(alias)
      const guid = nTable && nTable.guid
      x.parameter_value[0].table_guid = guid
      if (x.converted_columns.length > 0) {
        x.converted_columns.forEach((y) => {
          const convertedAlias = y.value.split('.')[0]
          const convertenTable = this.getTableByAlias(convertedAlias)
          const convertedGuid = convertenTable && convertenTable.guid
          y.table_guid = convertedGuid
        })
      }
    })
    // render table index Columns
    this.tableIndexColumns = this.tableIndexColumns.filter((x) => {
      let alias = x.column.split('.')[0]
      let guid = this._cacheAliasAndGuid(alias)
      x.table_guid = guid
      return x
    })
    // render partition desc
    if (this.partition_desc.table) {
      let guid = this._cacheAliasAndGuid(this.partition_desc.table)
      this.partition_desc.table_guid = guid
    }
  }
  // 自动布局
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
  // 连线
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
      this.setOverLayLabel(hasConn)
      this.plumbTool.refreshPlumbInstance()
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
          return reject('noFact')
        }
        metaData.join_tables = this._generateLookups()
        metaData.all_named_columns = this._generateAllColumns()
        metaData.simplified_measures = this._generateAllMeasureColumns()
        metaData.computed_columns = objectClone(this.computed_columns)
        metaData.last_modified = this.last_modified
        metaData.filter_condition = this.filter_condition
        metaData.partition_desc = this.partition_desc
        metaData.maintain_model_type = this._mount.maintain_model_type
        metaData.management_type = this.management_type
        metaData.canvas = this._generateTableRectData()
        // metaData = _filterData(metaData)
        resolve(metaData)
      } catch (e) {
        reject(e)
      }
    })
  }
  _renderTable () {
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
  // 批量连线
  _renderLinks () {
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
  _guidCache = {}
  _cacheAliasAndGuid (alias) {
    let guid = ''
    if (this._guidCache[alias]) {
      guid = this._guidCache[alias]
    } else {
      let ntable = this.getTableByAlias(alias)
      if (ntable) {
        this._guidCache[alias] = ntable.guid
        guid = this._guidCache[alias]
      }
    }
    return guid
  }
  _generateLookups () {
    let result = []
    for (let key in this.tables) {
      let t = this.tables[key]
      if (t.alias !== this.fact_table) {
        var joinInfo = t.getMetaJoinInfo(this)
        if (joinInfo) {
          result.push(joinInfo)
        }
      }
    }
    return result
  }
  _generateAllColumns () {
    let allNamedColumns = objectClone([...this._mount.dimensions, ...this.tableIndexColumns])
    // 移除前端业务字断
    allNamedColumns.forEach((col) => {
      delete col.guid
      delete col.table_guid
      delete col.isCC
      delete col.cc
    })
    return allNamedColumns
  }
  _generateAllMeasureColumns () {
    let allMeasures = objectClone(this._mount.all_measures)
    // 移除前端业务字断
    allMeasures.forEach((col) => {
      delete col.parameter_value[0].table_guid
      if (col.converted_columns && col.converted_columns.length) {
        col.converted_columns.forEach((k) => {
          delete k.table_guid
        })
      }
    })
    return allMeasures
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
  getAllConnectsByAlias (guid) {
    let result = []
    var reg = new RegExp('^' + guid + '\\$|\\$' + guid + '$')
    for (let i in this.allConnInfo) {
      if (reg.test(i)) {
        result.push(this.allConnInfo[i])
      }
    }
    return result
  }
  _replaceAlias (alias, fullName) {
    return alias + '.' + fullName.split('.')[1]
  }
  // private 更新所有measure里的alias
  _updateAllMeasuresAlias () {
    this.all_measures.forEach((x) => {
      const guid = x.parameter_value[0].table_guid
      const nTable = guid && this.getTableByGuid(guid)
      if (nTable) {
        const finalAlias = nTable.alias
        x.parameter_value[0].value = finalAlias + '.' + x.parameter_value[0].value.split('.')[1]
      }
      if (x.converted_columns.length > 0) {
        x.converted_columns.forEach((y) => {
          const convertedGuid = y.table_guid
          const convertedNTable = convertedGuid && this.getTableByGuid(convertedGuid)
          if (convertedNTable) {
            const convertedAlias = convertedNTable.alias
            y.value = convertedAlias + '.' + y.value.split('.')[1]
          }
        })
      }
    })
  }
  // 重新调整alias导致数据改变
  _changeAliasRelation () {
    // 更新join信息
    Object.values(this.tables).forEach((t) => {
      t.changeJoinAlias(this)
    })
    let replaceFuc = (x, key) => {
      let guid = x.table_guid
      let ntable = this.getTableByGuid(guid)
      x.column = this._replaceAlias(ntable.alias, x.column)
    }
    // 改变dimension列的alias
    this._mount.dimensions.forEach(replaceFuc)
    // 改变tableindex列的alias
    this.tableIndexColumns.forEach(replaceFuc)
    // 改变可计算列的alias
    this._mount.computed_columns.forEach((x) => {
      let guid = x.table_guid
      let ntable = this.getTableByGuid(guid)
      if (ntable) {
        x.tableAlias = ntable.alias
      }
    })
    this._updateAllMeasuresAlias()
  }
  // 别名修改
  changeAlias () {
    this._changeAliasRelation()
  }
  // 用户修改fact的时候，将所有的fact上的cc转移到另外一个fact上去
  moveCCToOtherFact () {

  }
  // 删除conn相关的主键的连接信息
  removeRenderLink (conn) {
    var fid = conn.sourceId
    var pid = conn.targetId
    delete this.allConnInfo[pid + '$' + fid]
    this.plumbTool.deleteConnect(conn)
    this.tables[pid].joinInfo = {}
  }
  _renderLabels () {
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
    return [].concat(stables, smeasures, sdimensions, sjoins, scolumns)
  }
  // search
  searchTable (keywords) {
    let filterResult = Object.values(this.tables).filter((x) => {
      return this.searchRule(x.alias, keywords)
    })
    return this.mixResult(filterResult, 'table', 'alias')
  }
  searchMeasure (keywords) {
    let filterResult = this._mount.all_measures.filter((x) => {
      return this.searchRule(x.name, keywords)
    })
    return this.mixResult(filterResult, 'measure', 'name')
  }
  searchDimension (keywords) {
    let filterResult = this._mount.dimensions.filter((x) => {
      return this.searchRule(x.name, keywords)
    })
    return this.mixResult(filterResult, 'dimension', 'name')
  }
  searchJoin (keywords) {
    let joinReg = /^(join|left\s*join|inner\s*join)$/i
    let leftJoinReg = /^left\s*join$/i
    let innerJoinReg = /^inner\s*join$/i
    let filterResult = []
    if (joinReg.test(keywords)) {
      Object.values(this.allConnInfo).forEach((conn) => {
        let pguid = conn.targetId
        let ptable = this.getTableByGuid(pguid)
        let joinInfo = ptable.getJoinInfo()
        if (leftJoinReg.test(keywords)) {
          if (joinInfo.join.type === 'LEFT') {
            filterResult.push(ptable)
          }
        } else if (innerJoinReg.test(keywords)) {
          if (joinInfo.join.type === 'INNER') {
            filterResult.push(ptable)
          }
        } else {
          filterResult.push(ptable)
        }
      })
    }
    return this.mixResult(filterResult, 'join', 'alias')
  }
  searchColumn (keywords) {
    var columnsResult = []
    for (var i in this.tables) {
      let columns = this.tables[i].columns
      columns && columns.forEach((co) => {
        co.full_colname = this.tables[i].alias + '.' + co.name
        co.table_guid = this.tables[i].guid
      })
      columnsResult.push(...columns)
    }
    columnsResult = columnsResult.filter((col) => {
      return this.searchRule(col.full_colname, keywords)
    })
    return this.mixResult(columnsResult, 'column', 'full_colname', keywords)
  }
  searchRule (content, keywords) {
    var reg = new RegExp(keywords, 'i')
    return reg.test(content)
  }
  // 混合结果信息
  mixResult (data, kind, key) {
    let result = []
    let actionsConfig = modelRenderConfig.searchAction[kind]
    actionsConfig.forEach((a) => {
      let i = 0
      data && data.forEach((t) => {
        if (i++ < modelRenderConfig.searchCountLimit) {
          let item = this.renderSearchResult(t, key, kind, a)
          if (item) {
            result.push(item)
          }
        }
      })
    })
    return result
  }
  // 数据结构定制化
  renderSearchResult (t, key, kind, a) {
    let item = {name: t[key], kind: kind, action: a.action, i18n: a.i18n, more: t}
    if (kind === 'table' && a.action === 'tableeditjoin' || kind === 'join' && a.action === 'editjoin') {
      let joinInfo = t.joinInfo[t.guid]
      if (joinInfo) {
        item.extraInfo = ' <span class="jtk-overlay">' + joinInfo.join.type + '</span> ' + joinInfo.foreignTable.name
      } else {
        return ''
      }
    }
    return item
  }
  checkTableCanSwitchFact (guid) {
    let ntable = this.getTableByGuid(guid)
    let factTable = this.getFactTable()
    if (factTable) {
      return false
    }
    if (ntable && ntable.getJoinInfo()) {
      return false
    }
    return true
  }
  checkTableCanDel (guid) {
    if (this._checkTableUseInConn(guid)) {
      return false
    }
    if (this._checkTableUseInDimension(guid)) {
      return false
    }
    if (this._checkTableUseInMeasure(guid)) {
      return false
    }
    if (this._checkTableUseInCC(guid)) {
      return false
    }
    if (this._checkTableUseInPartition(guid)) {
      return false
    }
    return true
  }
  _checkTableUseInConn (guid) {
    let conns = this.getAllConnectsByAlias(guid)
    if (conns.length) {
      return true
    }
  }
  _checkTableUseInDimension (guid) {
    return indexOfObjWithSomeKey(this.dimensions, 'table_guid', guid) >= 0
  }
  _checkTableUseInMeasure (guid) {
    let checkUsed = false
    this._mount.all_measures.forEach((measure) => {
      if (!checkUsed) {
        if (indexOfObjWithSomeKey(measure.parameter_value, 'table_guid', guid) >= 0) {
          checkUsed = true
        } else if (measure.converted_columns && measure.converted_columns.length) {
          checkUsed = indexOfObjWithSomeKey(measure.converted_columns, 'table_guid', guid) >= 0
        } else {
          checkUsed = false
        }
      }
    })
    return checkUsed
  }
  _checkTableUseInCC (guid) {
    return indexOfObjWithSomeKey(this._mount.computed_columns, 'table_guid', guid) >= 0
  }
  _checkTableUseInPartition (guid) {
    return this.partition_desc.table_guid === guid
  }
  delTable (guid) {
    return new Promise((resolve, reject) => {
      let conns = this.getAllConnectsByAlias(guid)
      conns && conns.forEach((conn) => {
        this.removeRenderLink(conn)
      })
      this._delTableRelated(guid)
      this.$delete(this.tables, guid)
      return resolve()
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
      // 删除对应的 tableindex
      this._delTableIndexByAlias(alias)
      // 删除对应的 cc
      // this._delCCByAlias(alias)
      // 删除对应的partition
      if (this.partition_desc.table_guid === guid) {
        this.partition_desc.table_guid = null
        this.partition_desc.partition_date_column = null
        this.partition_desc.partition_time_column = null
      }
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
  getComputedColumns () {
    return this._mount.computed_columns
  }
  _updateAllMeasuresCCToNewFactTable () {
    let factTable = this.getFactTable()
    this.all_measures.forEach((x) => {
      let cc = this.getCCObj(x.parameter_value[0].value)
      if (cc && factTable) {
        x.parameter_value[0].value = factTable.alias + '.' + x.parameter_value[0].value.split('.')[1]
        x.parameter_value[0].table_guid = factTable.guid
      }
      if (x.converted_columns.length > 0) {
        x.converted_columns.forEach((y) => {
          let cc = this.getCCObj(y.value)
          if (cc && factTable) {
            y.table_guid = factTable.guid
            y.value = factTable.alias + '.' + y.value.split('.')[1]
          }
        })
      }
    })
  }
  _updateAllNamedColumnsCCToNewFactTable () {
    let factTable = this.getFactTable()
    let replaceFuc = (x, key) => {
      let cc = this.getCCObj(x.column)
      if (cc && factTable) {
        x.column = this._replaceAlias(factTable.alias, x.column)
        x.table_guid = factTable.guid
      }
    }
    this._mount.dimensions.forEach(replaceFuc)
    // 改变tableindex列的alias
    this.tableIndexColumns.forEach(replaceFuc)
  }
  _updateCCToNewFactTable () {
    let factTable = this.getFactTable()
    if (factTable) {
      this._mount.computed_columns.forEach((x) => {
        x.table_guid = factTable.guid
        x.tableIdentity = factTable.name
        x.tableAlias = factTable.tableAlias
      })
    }
  }
  changeTableType (t) {
    t.kind = t.kind === modelRenderConfig.tableKind.fact ? modelRenderConfig.tableKind.lookup : modelRenderConfig.tableKind.fact
    this.setUniqueAlias(t)
    // 如何切换的是fact
    if (t.kind === modelRenderConfig.tableKind.fact) {
      // 将所有和fact相关的ccdimension，ccmeasure，cctableindex,cclist 换上新的fact 指纹
      this._updateAllMeasuresCCToNewFactTable()
      this._updateAllNamedColumnsCCToNewFactTable()
      this._updateCCToNewFactTable()
      this.fact_table = t.name
    }
    // 改变别名且替换掉所有关联的别名信息
    this.changeAlias()
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
  // 放大视图
  addZoom () {
    var nextZoom = this._mount.zoom + 1 > 10 ? 10 : this._mount.zoom += 1
    this.plumbTool.setZoom(nextZoom / 10)
    this.getZoomSpace()
  }
  // 缩小视图
  reduceZoom () {
    var nextZoom = this._mount.zoom - 1 < 4 ? 4 : this._mount.zoom -= 1
    this.plumbTool.setZoom(nextZoom / 10)
    this.getZoomSpace()
  }
  getZoomSpace () {
    if (this.renderDom) {
      this._mount.zoomXSpace = $(this.renderDom).width() * (1 - this._mount.zoom / 10) / 2
      this._mount.zoomYSpace = $(this.renderDom).height() * (1 - this._mount.zoom / 10) / 2
    }
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
      options._parent = this._mount
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
    let getTableBySource = () => {
      if (this.datasource) {
        let i = indexOfObjWithSomeKey(this.datasource, 'table', tableFullName)
        if (i >= 0) {
          return this.datasource[i]
        }
      }
      return []
    }
    let tableNamed = tableFullName.split('.')
    const currentDatasource = this.globalDataSource[this.project]
    if (currentDatasource) {
      let i = indexOfObjWithSomeKeys(currentDatasource, 'database', tableNamed[0], 'name', tableNamed[1])
      if (i >= 0) {
        let globalTableInfo = currentDatasource[i]
        globalTableInfo.table = globalTableInfo.database + '.' + globalTableInfo.name
        let k = indexOfObjWithSomeKey(this.datasource, 'table', tableFullName)
        if (k < 0) {
          this.datasource.push(globalTableInfo)
        }
        return globalTableInfo
      } else {
        return getTableBySource()
      }
    }
    return getTableBySource()
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
  getCCObj () {
    let column = ''
    let alias = ''
    if (arguments.length === 1) {
      let named = arguments[0].split('.')
      alias = named[0]
      column = named[1]
    } else if (arguments.length === 2) {
      alias = arguments[0]
      column = arguments[1]
    }
    let i = indexOfObjWithSomeKeys(this.computed_columns, 'columnName', column, 'tableAlias', alias)
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
  editDimension (dimension) {
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
  _delTableIndexByAlias (alias) {
    let tableIndexColumns = this.tableIndexColumns.filter((item) => {
      return item.column && item.column.split('.')[0] !== alias
    })
    this.tableIndexColumns.splice(0, this.tableIndexColumns.length)
    this.tableIndexColumns.push(...tableIndexColumns)
  }
  // measure parameterValue 临时结构
  _delMeasureByAlias (alias) {
    let measures = this._mount.all_measures.filter((item) => {
      if (item.parameter_value[0].type === 'constant' || item.parameter_value[0] && item.parameter_value[0].type === 'column' && item.parameter_value[0].value.split('.')[0] !== alias) {
        if (item.converted_columns && item.converted_columns.length > 0) {
          const finalConvertedColumns = item.converted_columns.filter((column) => {
            return column.type === 'constant' || column.type === 'column' && column.value.split('.')[0] !== alias
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
  delMeasure (i) {
    this._mount.all_measures.splice(i, 1)
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
  // 检查是否有同名, 通过重名检测返回true
  checkSameCCName (name) {
    return indexOfObjWithSomeKey(this._mount.computed_columns, 'columnName', name) < 0
  }
  generateCCMeta (ccObj) {
    let factTable = this.getFactTable()
    if (factTable) {
      let ccBase = {
        tableIdentity: factTable.name,
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
      if (this.checkSameCCName(ccObj.columnName)) {
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
  // 删除可计算列
  delCC (ccObj) {
    return new Promise((resolve) => {
      for (let i = 0; i < this._mount.computed_columns.length; i++) {
        const c = this._mount.computed_columns[i]
        if (c.guid === ccObj.guid) {
          this.delCCByIndex(i)
          resolve(c)
          break
        }
      }
    })
  }
  // 按照索引删除可计算列
  delCCByIndex (i) {
    this._mount.computed_columns.splice(i, 1)
  }
  // 自动布局
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
  // 添加连接点
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
  // 添加连线上的图标（连接类型Left/Inner）
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
