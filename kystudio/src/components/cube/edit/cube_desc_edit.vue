<template>
  <div id="cube-edit">
    <div class="cube-step">
      <el-steps  :active="activeStep" space="20%" finish-status="finish" process-status="wait" center align-center>
        <el-step :title="$t('cubeInfo')" @click.native="step(1)"></el-step>
        <el-step :title="$t('dimensions')" @click.native="step(2)"></el-step>
        <el-step :title="$t('measures')" @click.native="step(3)"></el-step>
        <el-step :title="$t('tableIndex')" @click.native="step(4)"></el-step>
        <el-step :title="$t('overview')" @click.native="step(5)"></el-step>
      </el-steps>
    </div>

    <div class="line margin-l-r"></div>
    <div class="ksd-mt-10 ksd-mb-10" id="cube-main">
    <info ref="infoForm" v-if="activeStep===1" :cubeDesc="cubeDetail" :cubeInstance="extraoption.cubeInstance" :modelDesc="modelDetail" :isEdit="isEdit" :sampleSql="sampleSql"></info>
    <dimensions v-if="activeStep===2" :cubeDesc="cubeDetail" :cubeInstance="extraoption.cubeInstance" :modelDesc="modelDetail" :isEdit="isEdit"></dimensions>
    <measures v-if="activeStep===3" :cubeDesc="cubeDetail" :cubeInstance="extraoption.cubeInstance" :modelDesc="modelDetail" :isEdit="isEdit"></measures>
    <table_index v-if="activeStep===4" :cubeDesc="cubeDetail" :cubeInstance="extraoption.cubeInstance" :isEdit="isEdit" :modelDesc="modelDetail" :rawTable="rawTable"></table_index>
    <overview v-if="activeStep===5" :cubeDesc="cubeDetail" :cubeInstance="extraoption.cubeInstance" :modelDesc="modelDetail"></overview>
    </div>

    <el-button class="button_right" type="primary" v-if="activeStep !== 5" @click.native="next">{{$t('next')}}<i class="el-icon-arrow-right el-icon--right"></i></el-button>
    <el-button class="button_right" :loading="cubeSaving" type="primary" v-if="activeStep === 5" @click.native="saveOrUpdate">{{$t('save')}}</el-button>
    <el-button class="button_right" icon="arrow-left" v-if="activeStep !== 1" @click.native="prev">{{$t('prev')}}</el-button>

    <el-dialog :title="$t('errorMsg')" v-model="showErrorVisible">
      <el-alert
        :title="errorMsg"
        type="error"
        :closable="false">
      </el-alert>
      <json :json="cubeDetail"></json>
      <div slot="footer" class="dialog-footer">
        <el-button @click="showErrorVisible = false">{{$t('yes')}}</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import { mapActions, mapMutations } from 'vuex'
import info from './info_edit'
import dimensions from './dimensions_edit'
import measures from './measures_edit'
import tableIndex from './table_index_edit'
import overview from './overview_edit'
import json from '../json'
import { removeNameSpace, getNameSpace, objectClone } from '../../../util/index'
import { handleSuccess, handleError, kapConfirm, loadBaseEncodings, transToGmtTime } from '../../../util/business'
export default {
  name: 'cubeDescEdit',
  props: ['extraoption'],
  data () {
    return {
      cubeSaveST: null,
      activeStep: 1,
      cubeSaving: false,
      cubeDraftSaving: false,
      isEdit: this.extraoption.isEdit,
      hisCubeMetaStr: '',
      hisRawTableStr: '',
      renderCubeFirst: false,
      aliasMap: {},
      index: 0,
      saveConfig: {
        timer: 0,
        limitTimer: 5
      },
      modelDetail: {},
      cubeDetail: {},
      rawTable: {
        needDelete: false,
        tableDetail: {
          columns: [],
          name: '',
          model_name: '',
          engine_type: '',
          storage_type: ''
        }
      },
      sampleSql: {sqlString: '', sqlList: [], sqlSize: 0},
      selected_project: this.extraoption.project,
      wizardSteps: [
        {title: 'checkCubeInfo', isComplete: false},
        {title: 'checkDimensions', isComplete: false},
        {title: 'checkMeasures', isComplete: false},
        {title: 'checkTableIndex', isComplete: false}
      ],
      showErrorVisible: false,
      errorMsg: '',
      oldMetadata: {
        cube: {},
        tableIndex: {
          columns: []
        },
        sampleSql: {}
      }
    }
  },
  components: {
    'info': info,
    'dimensions': dimensions,
    'measures': measures,
    'table_index': tableIndex,
    'overview': overview,
    'json': json
  },
  methods: {
    ...mapActions({
      checkCubeNameAvailability: 'CHECK_CUBE_NAME_AVAILABILITY',
      loadCubeDesc: 'LOAD_CUBE_DESC',
      updateCube: 'UPDATE_CUBE',
      loadModelInfo: 'LOAD_MODEL_INFO',
      saveSampleSql: 'SAVE_SAMPLE_SQL',
      getRawTableDesc: 'GET_RAW_TABLE_DESC',
      updateRawTable: 'UPDATE_RAW_TABLE',
      saveRawTable: 'SAVE_RAW_TABLE',
      deleteRawTable: 'DELETE_RAW_TABLE',
      draftCube: 'DRAFT_CUBE',
      getSql: 'GET_SAMPLE_SQL',
      loadDataSourceByProject: 'LOAD_DATASOURCE'
    }),
    ...mapMutations({
      cacheRawTableBaseData: 'CACHE_RAWTABLE__BASEDATA'
    }),
    step: function (num) {
      this.activeStep = this.stepsCheck(num)
    },
    prev: function () {
      this.activeStep = this.activeStep - 1
    },
    next: function () {
      this.wizardSteps[this.activeStep - 1].isComplete = this.checkCubeSteps(this.activeStep)
      if (this.wizardSteps[this.activeStep - 1].isComplete) {
        this.activeStep = this.activeStep + 1
      }
    },
    stepsCheck: function (num) {
      for (let i = 1; i < num; i++) {
        this.wizardSteps[i - 1].isComplete = this.checkCubeSteps(i)
        if (!this.wizardSteps[i - 1].isComplete) {
          return i
        }
      }
      return num
    },
    checkCubeSteps: function (index) {
      switch (index) {
        case 1:
          return this.checkCubeInfo()
        case 2:
          return this.checkDimensions()
        case 3:
          return this.checkMeasures()
        case 4:
          return this.checkTableIndex()
        default:
          return true
      }
    },
    checkCubeInfo: function () {
      let nameUsed = false
      if (!this.isEdit) {
        this.checkCubeNameAvailability({cubeName: this.cubeDetail.name, project: this.selected_project}).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            if (data === false) {
              this.$message({
                showClose: true,
                duration: 3000,
                message: this.$t('checkCubeNamePartOne') + this.cubeDetail.name.toUpperCase() + this.$t('checkCubeNamePartTwo'),
                type: 'error'
              })
              nameUsed = true
            }
          })
        }).catch((res) => {
          // handleError(res)
        })
      }
      if (nameUsed) {
        return false
      } else {
        return true
      }
    },
    checkDimensions: function () {
      if (this.cubeDetail.dimensions.length <= 0) {
        this.$message({
          showClose: true,
          duration: 3000,
          message: this.$t('checkDimensions'),
          type: 'error'
        })
        return false
      }
      for (let j = 0; j < this.cubeDetail.aggregation_groups.length; j++) {
        if (!this.cubeDetail.aggregation_groups[j] || !this.cubeDetail.aggregation_groups[j].includes || this.cubeDetail.aggregation_groups[j].includes.length === 0) {
          this.$message({
            showClose: true,
            duration: 3000,
            message: this.$t('checkAggGroup'),
            type: 'error'
          })
          return false
        }
      }
      let shardRowkeyList = []
      for (let i = 0; i < this.cubeDetail.rowkey.rowkey_columns.length; i++) {
        if (this.cubeDetail.rowkey.rowkey_columns[i].isShardBy === true) {
          shardRowkeyList.push(this.cubeDetail.rowkey.rowkey_columns[i].column)
        }
        if (this.cubeDetail.rowkey.rowkey_columns[i].encoding.substr(0, 3) === 'int' && (this.cubeDetail.rowkey.rowkey_columns[i].encoding.substr(4) < 1 || this.cubeDetail.rowkey.rowkey_columns[i].encoding.substr(4) > 8)) {
          this.$message({
            showClose: true,
            duration: 3000,
            message: this.$t('checkRowkeyInt'),
            type: 'error'
          })
          return false
        }
      }
      if (shardRowkeyList.length > 1) {
        this.$message({
          showClose: true,
          message: this.$t('checkRowkeyShard'),
          type: 'error'
        })
        return false
      }
      return true
    },
    checkMeasures: function () {
      let existCountExpression = false
      let countDistinctColumns = []
      for (let i = 0; i < this.cubeDetail.measures.length; i++) {
        if (this.cubeDetail.measures[i].function.expression === 'COUNT') {
          existCountExpression = true
        }
        if (this.cubeDetail.measures[i].function.expression === 'COUNT_DISTINCT' && this.cubeDetail.measures[i].function.returntype === 'bitmap') {
          countDistinctColumns.push(this.cubeDetail.measures[i].function.parameter.value)
        }
        if (this.cubeDetail.measures[i].function.returntype === '') {
          this.$message({
            showClose: true,
            message: this.$t('returnTypeNullPartOne') + this.cubeDetail.measures[i].name + this.$t('returnTypeNullPartTwo'),
            type: 'error'
          })
          return false
        }
      }
      if (countDistinctColumns.length > 0) {
        for (let i = 0; i < this.cubeDetail.dimensions.length; i++) {
          let dimension = this.cubeDetail.dimensions[i]
          if (countDistinctColumns.indexOf(dimension.table + '.' + dimension.column) >= 0) {
            this.$confirm(this.$t('checkCountDistinctPartOne') + dimension.table + '.' + dimension.column + this.$t('checkCountDistinctPartTwo') + this.$t('checkCountDistinctPartThree') + dimension.table + '.' + dimension.column + this.$t('checkCountDistinctPartFour'), this.$t('errorMsg'), {
              showCancelButton: false,
              showConfirmButton: false,
              type: 'error'
            })
            return false
          }
        }
      }
      if (!existCountExpression) {
        this.$message({
          showClose: true,
          duration: 3000,
          message: this.$t('checkMeasuresCount'),
          type: 'error'
        })
        return false
      }
      let cfMeasures = []
      if (this.cubeDetail.hbase_mapping) {
        this.cubeDetail.hbase_mapping.column_family.forEach(function (cf) {
          cf.columns[0].measure_refs.forEach(function (measure, index) {
            cfMeasures.push(measure)
          })
        })
        if (cfMeasures.length !== this.cubeDetail.measures.length) {
          this.$message({
            showClose: true,
            duration: 3000,
            message: this.$t('checkColumnFamily'),
            type: 'error'
          })
          return false
        }
        for (let j = 0; j < this.cubeDetail.hbase_mapping.column_family.length; j++) {
          if (this.cubeDetail.hbase_mapping.column_family[j].columns[0].measure_refs.length === 0) {
            this.$message({
              showClose: true,
              duration: 3000,
              message: this.$t('checkColumnFamilyNull'),
              type: 'error'
            })
            return false
          }
        }
      }
      return true
    },
    checkTableIndex: function () {
      if (this.rawTable.tableDetail.columns.length === 0) {
        return true
      }
      let sortedCount = 0
      let shardCount = 0
      let setTypeError = false
      let fuzzyTypeError = false
      for (let i = 0; i < this.rawTable.tableDetail.columns.length; i++) {
        var curColumn = this.rawTable.tableDetail.columns[i]
        if (curColumn.is_sortby === true) {
          let _encoding = this.getEncoding(curColumn.encoding)
          if (['date', 'time', 'integer'].indexOf(_encoding) < 0) {
            if (sortedCount === 0) {
              setTypeError = true
            }
          }
          sortedCount++
        }
        var columnType = this.modelDetail.columnsDetail[curColumn.table + '.' + curColumn.column].datatype || ''
        // 检测fazzy下 必须选择datatype 为varchar类型的列
        if (curColumn.index === 'fuzzy' && columnType.indexOf('varchar') === -1) {
          fuzzyTypeError = true
        }
        if (curColumn.is_shardby === true) {
          shardCount++
        }
      }
      if (fuzzyTypeError) {
        this.$message({
          showClose: true,
          duration: 3000,
          message: this.$t('fuzzyTip'),
          type: 'error'
        })
        return false
      }
      if (setTypeError) {
        this.$message({
          showClose: true,
          duration: 3000,
          message: this.$t('rawtableSortedWidthDate'),
          type: 'error'
        })
        return false
      }
      if (shardCount > 1) {
        this.$message({
          showClose: true,
          duration: 3000,
          message: this.$t('shardCountError'),
          type: 'error'
        })
        return false
      }
      if (sortedCount === 0) {
        this.$message({
          showClose: true,
          duration: 3000,
          message: this.$t('rawtableSetSorted'),
          type: 'error'
        })
        return false
      } else {
        return true
      }
    },
    getModelColumnsDataForRawtable: function () {
      var modelColumnsData = []
      let baseEncodings = loadBaseEncodings(this.$store.state.datasource)
      this.modelDetail.dimensions.forEach((dimension) => {
        dimension.columns.forEach((column) => {
          let index = 'discrete'
          let sorted = false
          if (this.modelDetail.partition_desc && dimension.table + '.' + column === this.modelDetail.partition_desc.partition_date_column) {
            sorted = true
          }
          var columType = this.modelDetail.columnsDetail[dimension.table + '.' + column] && this.modelDetail.columnsDetail[dimension.table + '.' + column].datatype
          let encodingVersion = 1
          if (['time', 'date', 'integer'].indexOf(columType) < 0) {
            columType = ''
          }
          if (columType === 'integer') {
            columType = columType + ':4'
          }
          encodingVersion = baseEncodings.getEncodingMaxVersion(columType)
          modelColumnsData.push({
            index: index,
            encoding: columType || 'orderedbytes',
            table: dimension.table,
            column: column,
            encoding_version: encodingVersion,
            is_sortby: sorted,
            is_shardby: false
          })
        })
      })
      this.modelDetail.metrics.forEach((measure) => {
        let index = 'discrete'
        let sorted = false
        if (this.modelDetail.partition_desc && measure === this.modelDetail.partition_desc.partition_date_column) {
          sorted = true
        }
        var tableName = getNameSpace(measure)
        var columnName = removeNameSpace(measure)
        var columType = this.modelDetail.columnsDetail[tableName + '.' + columnName] && this.modelDetail.columnsDetail[tableName + '.' + columnName].datatype
        let encodingVersion = 1
        if (['time', 'date', 'integer'].indexOf(columType) < 0) {
          columType = ''
        }
        if (columType === 'integer') {
          columType = columType + ':4'
        }
        encodingVersion = baseEncodings.getEncodingMaxVersion(columType)
        modelColumnsData.push({
          index: index,
          encoding: columType || 'orderedbytes',
          table: tableName,
          column: columnName,
          encoding_version: encodingVersion,
          is_sortby: sorted,
          is_shardby: false
        })
      })
      return modelColumnsData
    },
    saveDraft (tipChangestate) {
      if (!this.checkHasChanged()) {
        if (tipChangestate) {
          this.$message({
            type: 'warning',
            message: '未检测到任何改动!'
          })
        }
        return
      }
      if (this.cubeSaving) {
        return
      }
      this.cubeDraftSaving = true
      if (+this.cubeDetail.engine_type === 100 || +this.cubeDetail.engine_type === 99) {
        this.rawTable.tableDetail.name = this.cubeDetail.name
        this.rawTable.tableDetail.model_name = this.cubeDetail.model_name
        this.rawTable.tableDetail.engine_type = this.cubeDetail.engine_type
        this.rawTable.tableDetail.storage_type = this.cubeDetail.storage_type
      }
      var saveData = {
        cubeDescData: JSON.stringify(this.cubeDetail),
        project: this.selected_project
      }
      // 将cube的起始时间设置转换成UTC
      var cloneCubeDescData = JSON.parse(saveData.cubeDescData)
      if (cloneCubeDescData && (+cloneCubeDescData.engine_type === 100 || +cloneCubeDescData.engine_type === 99)) {
        delete cloneCubeDescData.hbase_mapping
      }
      saveData.cubeDescData = JSON.stringify(cloneCubeDescData)
      if (this.rawTable.tableDetail.columns && this.rawTable.tableDetail.columns.length) {
        saveData.rawTableDescData = JSON.stringify(this.rawTable.tableDetail)
      }
      this.draftCube(saveData).then((res) => {
        this.cubeDraftSaving = false
        handleSuccess(res, (data, code, status, msg) => {
          try {
            var cubeData = JSON.parse(data.cubeDescData)
            this.cubeDetail.uuid = cubeData.uuid
            this.cubeDetail.last_modified = cubeData.last_modified
          } catch (e) {
          }
          try {
            var rawTableData = JSON.parse(data.rawTableDescData)
            this.rawTable.tableDetail.uuid = rawTableData.uuid
            this.rawTable.tableDetail.last_modified = rawTableData.last_modified
          } catch (e) {
          }
          this.$emit('reload', 'cubeList')
        })
      }, (res) => {
        this.cubeDraftSaving = false
        handleError(res)
      })
    },
    timerSave () {
      this.cubeSaveST = setTimeout(() => {
        this.saveConfig.timer++
        if (this.saveConfig.timer > this.saveConfig.limitTimer && !this.cubeDraftSaving) {
          this.saveDraft()
          this.saveConfig.timer = 0
        }
        this.timerSave()
      }, 1000)
    },
    filterUnCheckObject (obj) {
      var newObj = objectClone(obj)
      delete newObj.last_modified
      delete newObj.status
      delete newObj.name
      delete newObj.uuid
      delete newObj.oldMeasures
      delete newObj.oldColumnFamily
      return JSON.stringify(newObj)
    },
    checkHasChanged () {
      var filterCubeMetaStr = this.filterUnCheckObject(this.cubeDetail)
      var filterRawTableStr = this.filterUnCheckObject(this.rawTable.tableDetail)
      if (this.renderCubeFirst) {
        this.renderCubeFirst = false
        this.hisCubeMetaStr = filterCubeMetaStr
        this.hisRawTableStr = filterRawTableStr
        return false
      }
      this.hisCubeMetaStr = filterCubeMetaStr
      this.hisRawTableStr = filterRawTableStr
      return true
    },
    saveCube () {
      if (this.cubeDraftSaving) {
        this.$message({
          type: 'warning',
          message: this.$t('kylinLang.common.saveDraft')
        })
        return
      }
      this.cubeSaving = true
      if (this.cubeDetail.engine_type === 100 || this.cubeDetail.engine_type === 99) {
        this.rawTable.tableDetail.name = this.cubeDetail.name
        this.rawTable.tableDetail.model_name = this.cubeDetail.model_name
        this.rawTable.tableDetail.engine_type = this.cubeDetail.engine_type
        this.rawTable.tableDetail.storage_type = this.cubeDetail.storage_type
      }
      var saveData = {
        cubeDescData: JSON.stringify(this.cubeDetail),
        project: this.selected_project
      }
      // 将cube的起始时间设置转换成UTC
      var cloneCubeDescData = JSON.parse(saveData.cubeDescData)
      if (cloneCubeDescData && (+cloneCubeDescData.engine_type === 100 || +cloneCubeDescData.engine_type === 99)) {
        delete cloneCubeDescData.hbase_mapping
      }
      saveData.cubeDescData = JSON.stringify(cloneCubeDescData)
      if (this.rawTable.tableDetail.columns && this.rawTable.tableDetail.columns.length) {
        saveData.rawTableDescData = JSON.stringify(this.rawTable.tableDetail)
      }
      this.updateCube(saveData).then((res) => {
        this.cubeSaving = false
        handleSuccess(res, (data, code, status, msg) => {
          const h = this.$createElement
          let createTime = transToGmtTime(parseInt(data.createTime))
          this.$msgbox({
            title: '消息',
            message: h('p', null, [
              h('span', null, this.$t('kylinLang.common.saveSuccess')),
              h('br', null),
              h('span', { style: 'color: #218fea' }, 'Version ' + data.version.substr(8) + '  ' + createTime)
            ]),
            showCancelButton: true,
            confirmButtonText: this.$t('kylinLang.common.ok'),
            cancelButtonText: this.$t('kylinLang.common.cancel'),
            callback: action => {
              this.$emit('reload', 'cubeList')
              this.$emit('removetabs', 'cube' + this.extraoption.cubeName, 'Overview')
            }
          })
        })
      }, (res) => {
        this.cubeSaving = false
        handleError(res)
      })
    },
    saveOrUpdate: function () {
      let oldCubeMeta = this.filterUnCheckObject(this.oldMetadata.cube)
      let oldTableIndexMeta = this.filterUnCheckObject(this.oldMetadata.tableIndex.columns)
      let oldSqlMeta = this.filterUnCheckObject(this.oldMetadata.sampleSql)
      let newCubeMeta = this.filterUnCheckObject(this.cubeDetail)
      let newTableIndexMeta = this.filterUnCheckObject(this.rawTable.tableDetail.columns)
      let newSqlMeta = this.filterUnCheckObject(this.sampleSql.sqlList)
      if (oldCubeMeta === newCubeMeta && oldTableIndexMeta === newTableIndexMeta && oldSqlMeta === newSqlMeta) {
        this.$emit('removetabs', 'cube' + this.extraoption.cubeName, 'Overview')
      } else {
        kapConfirm(this.$t('kylinLang.cube.saveCubeTip')).then(() => {
          this.saveCube()
        })
      }
    },
    getEncoding: function (encode) {
      let code = encode.split(':')
      return code[0]
    },
    createNewCube: function () {
      this.cubeDetail = {
        name: this.extraoption.cubeName,
        model_name: this.extraoption.modelName,
        description: '',
        dimensions: [],
        measures: [],
        rowkey: {
          rowkey_columns: []
        },
        aggregation_groups: [],
        dictionaries: [],
        partition_date_start: 0,
        partition_date_end: undefined,
        notify_list: [],
        hbase_mapping: {
          column_family: []
        },
        status_need_notify: ['ERROR', 'DISCARDED', 'SUCCEED'],
        retention_range: '0',
        auto_merge_time_ranges: [604800000, 2419200000],
        engine_type: this.getCubeEng(),
        storage_type: this.getStorageEng(),
        override_kylin_properties: {}
      }
    },
    getProperty: function (name) {
      let result = (new RegExp(name + '=(.*?)\\n')).exec(this.$store.state.system.serverConfig)
      return result && result[1] || ''
    },
    getCubeEng: function () {
      let CubeEng = this.getProperty('kylin.engine.default').trim()
      if (!CubeEng) {
        return 2
      }
      return CubeEng
    },
    getStorageEng: function () {
      let StorageEng = this.getProperty('kylin.storage.default').trim()
      if (!StorageEng) {
        return 2
      }
      return StorageEng
    },
    loadCubeDetail: function () {
      var _this = this
      this.createNewCube()
      this.loadCubeDesc({cubeName: this.extraoption.cubeName, project: this.extraoption.project}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.cubeDetail = data.cube || data.draft
          this.cubeDetail.name = this.extraoption.cubeName
          this.oldMetadata.cube = objectClone(data.cube) || {}
          if (data.cube && data.draft) {
            kapConfirm(this.$t('kylinLang.common.checkDraft'), {
              confirmButtonText: 'OK',
              cancelButtonText: 'NO'
            }).then(() => {
              this.cubeDetail = data.draft
              loadRowTable(true)
            }).catch(() => {
              this.cubeDetail = data.cube
              this.cubeDetail.status = this.extraoption.cubeStatus
              loadRowTable(false)
            })
          } else {
            if (data.cube) {
              this.cubeDetail.status = this.extraoption.cubeStatus
              loadRowTable(false)
            } else {
              loadRowTable(true)
            }
          }
          this.cubeDetail.oldMeasures = objectClone(this.cubeDetail.measures)
          this.cubeDetail.oldColumnFamily = objectClone(this.cubeDetail.hbase_mapping && this.cubeDetail.hbase_mapping.column_family || [])
          function loadRowTable (isDraft) {
            _this.$store.state.cube.cubeRowTableIsSetting = false
            _this.getRawTableDesc({cubeName: _this.extraoption.cubeName, project: _this.extraoption.project}).then((res) => {
              handleSuccess(res, (data, code, status, msg) => {
                var rawtableData = isDraft ? data.draft : data.rawTable
                _this.oldMetadata.tableIndex = objectClone(data.rawTable) || {columns: []}
                if (rawtableData) {
                  _this.$set(_this.rawTable, 'tableDetail', rawtableData)
                  _this.$store.state.cube.cubeRowTableIsSetting = true
                }
              })
            })
          }
        })
      }, (res) => {
        handleError(res)
      })
    },
    getTables: function () {
      let rootFactTable = removeNameSpace(this.modelDetail.fact_table)
      let factTables = []
      let lookupTables = []
      factTables.push(rootFactTable)
      this.$set(this.modelDetail, 'columnsDetail', {})
      this.$store.state.datasource.dataSource[this.selected_project].forEach((table) => {
        if (this.modelDetail.fact_table === table.database + '.' + table.name) {
          table.columns.forEach((column) => {
            this.$set(this.modelDetail.columnsDetail, rootFactTable + '.' + column.name, {
              name: column.name,
              datatype: column.datatype,
              cardinality: table.cardinality[column.name],
              comment: column.comment})
          })
          this.aliasMap[table.name] = table.database + '.' + table.name
        }
      })
      this.modelDetail.lookups.forEach((lookup) => {
        if (lookup.kind === 'FACT') {
          if (!lookup.alias) {
            lookup['alias'] = removeNameSpace(lookup.table)
          }
          factTables.push(lookup.alias)
          this.aliasMap[lookup.alias] = lookup.table
        } else {
          if (!lookup.alias) {
            lookup['alias'] = removeNameSpace(lookup.table)
          }
          lookupTables.push(lookup.alias)
          this.aliasMap[lookup.alias] = lookup.table
        }
        this.$store.state.datasource.dataSource[this.selected_project].forEach((table) => {
          if (lookup.table === table.database + '.' + table.name) {
            table.columns.forEach((column) => {
              this.$set(this.modelDetail.columnsDetail, lookup.alias + '.' + column.name, {
                name: column.name,
                datatype: column.datatype,
                cardinality: table.cardinality[column.name],
                comment: column.comment})
            })
          }
        })
      })
      if (this.modelDetail.computed_columns) {
        this.modelDetail.computed_columns.forEach((co) => {
          var alias = ''
          for (var i in this.aliasMap) {
            if (this.aliasMap[i] === co.tableIdentity) {
              alias = i
              this.$set(this.modelDetail.columnsDetail, alias + '.' + co.columnName, {
                name: co.columnName,
                datatype: co.datatype,
                cardinality: 'N/A',
                comment: co.expression
              })
            }
          }
        })
      }
      this.$set(this.modelDetail, 'lookupTables', lookupTables)
      this.$set(this.modelDetail, 'factTables', factTables)
    },
    loadSql: function () {
      this.getSql(this.cubeDetail.name).then((res) => {
        handleSuccess(res, (data) => {
          this.oldMetadata.sampleSql = objectClone(data.versioned_sqls) || {}
          this.sampleSql.sqlList = data.suggested_sqls || data.versioned_sqls
          this.sampleSql.sqlString = this.sampleSql.sqlList.join(';')
          this.sampleSql.sqlSize = this.sampleSql.sqlList.length
        })
      })
    }
  },
  created () {
    this.createNewCube()
    this.$store.state.cube.cubeRowTableIsSetting = false
    if (this.isEdit) {
      this.loadCubeDetail()
      this.loadSql()
    }
    this.loadDataSourceByProject({project: this.selected_project, isExt: true}).then(() => {
      this.loadModelInfo({modelName: this.extraoption.modelName, project: this.selected_project}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.renderCubeFirst = true
          this.modelDetail = data.model
          this.getTables()
          var rawtableBaseData = this.getModelColumnsDataForRawtable()
          this.cacheRawTableBaseData({project: this.selected_project, modelName: this.extraoption.modelName, data: rawtableBaseData})
          if (this.$refs.infoForm) {
            this.$refs.infoForm.getModelHelthInfo(this.selected_project, this.extraoption.modelName)
          }
        })
      }, (res) => {
        handleError(res)
      })
    }, (res) => {
      handleError(res)
    })
  },
  computed: {
    selected_cube: function () {
      return this.$store.state.cube.cubeAdd
    }
  },
  mounted () {
    this.timerSave()
  },
  destroyed () {
    clearTimeout(this.cubeSaveST)
  },
  locales: {
    'en': {cubeInfo: 'Cube Info', sampleSql: 'Sample Sql', dimensions: 'Dimensions', measures: 'Measures', refreshSetting: 'Refresh Setting', tableIndex: 'Table Index', AdvancedSetting: 'Advanced Setting', overview: 'Overview', prev: 'Prev', next: 'Next', save: 'Save', checkCubeNamePartOne: 'The CUBE named [ ', checkCubeNamePartTwo: ' ] already exists!', checkDimensions: 'Dimension can\'t be null!', checkAggGroup: 'Each aggregation group can\'t be empty!', checkMeasuresCount: '[ COUNT] metric is required!', checkRowkeyInt: 'int encoding column length should between 1 and 8!', checkRowkeyShard: 'At most one \'shard by\' column is allowed!', checkColumnFamily: 'All measures need to be assigned to column family!', checkColumnFamilyNull: 'Each column family can\'t not be empty!', checkCOKey: 'Property name is required!', checkCOValue: 'Property value is required!', rawtableSetSorted: 'You must set one column with an index value of sorted! ', rawtableSortedWidthDate: 'The first column with "sorted" index must be a column with "integer", "time" or "date" encoding! ', rawtableSingleSorted: 'Only one column is allowed to set with an index value of sorted! ', errorMsg: 'Error Message', shardCountError: 'Max shard by column number is 1', unsetScheduler: 'Scheduler configuration is imperfect', fuzzyTip: 'Fuzzy index can be applied to string(varchar) type data only.', checkCountDistinctPartOne: '[', checkCountDistinctPartTwo: '］is currently not supported as both a cube dimension and a count distinct(bitmap) measure.', checkCountDistinctPartThree: 'Please apply count distinct(hllc) instead or remove [', checkCountDistinctPartFour: '] from cube dimension list.', returnTypeNullPartOne: 'Measure[', returnTypeNullPartTwo: ']的返回类型为空，请重新设置该度量。'},
    'zh-cn': {cubeInfo: 'Cube信息', sampleSql: '查询样例', dimensions: '维度', measures: '度量', refreshSetting: '刷新设置', tableIndex: '表索引', AdvancedSetting: '高级设置', overview: '概览', prev: 'Prev', next: 'Next', save: 'Save', checkCubeNamePartOne: '名为 [ ', checkCubeNamePartTwo: '] 的CUBE已经存在!', checkDimensions: '维度不能为空!', checkAggGroup: '任意聚合组不能为空!', checkMeasuresCount: '[ COUNT] 度量是必须的!', checkRowkeyInt: '编码为int的列的长度应该在1-8之间!', checkRowkeyShard: '最多只允许一个\'shard by\'的列!', checkColumnFamily: '所有度量都需要被分配到列族中!', checkColumnFamilyNull: '任一列族不能为空!', rawtableSetSorted: '必须设置一个列的index的值为sorted! ', rawtableSortedWidthDate: '第一个sorted列必须是编码为integer、date或time的列', rawtableSingleSorted: '只允许设置一个列的index的值为sorted', errorMsg: '错误信息', shardCountError: 'Shard by最多可以设置一列', unsetScheduler: 'Scheduler 参数设置不完整', fuzzyTip: '模糊(fuzzy)索引只支持应用于string（varchar）类型数据。', checkCountDistinctPartOne: '当前版本中，[', checkCountDistinctPartTwo: ']无法既做cube维度也作为count distinct(bitmap) 的度量参数。', checkCountDistinctPartThree: '请使用count distinct(hllc) 替换该度量，或从cube维度列表中删除[', checkCountDistinctPartFour: ']。', returnTypeNullPartOne: '度量[', returnTypeNullPartTwo: ']的返回类型为空，请重新设置该度量。'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .button_right {
    float: right;
    margin-left:10px;
    margin-bottom: 20px;
  }
  #cube-edit{
    padding: 0 30px 0 30px;
    .cube-step {
      width:100%;
      padding-top:30px;
    }
    .el-input__inner{
      border-color: @grey-color;
    }
    .button_right{
      background: transparent;
      border-color: @grey-color;
    }
    .button_right:hover{
      border-color: @base-color;
    }
    .el-step__main{
      width: 188px;
      transform: translateX(-50%);
      margin-left: 14px!important;
      text-align: center;
      margin-right: 0!important;
      padding: 0;
    }
    .el-alert--info {
      width: 70%;
      background-color: rgba(33,143,234,0.1);
      color: rgb(33,143,234);
      font-weight:bold;
      border: 1px solid rgba(33,143,234,0.2);
    }
  }
  #cube-main{
    *{
      font-size: 12px;
    }
  }
</style>
