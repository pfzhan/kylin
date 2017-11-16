<template>
<div class="paddingbox" style="padding: 0 30px 0 30px;" id="cube-edit">
  <div>
    <el-steps id="cube-step" :active="activeStep" space="18%" finish-status="finish" process-status="wait" center align-center style="width:100%;margin:0 auto;padding-top:30px;margin-left: 14px;">
      <el-step :title="$t('cubeInfo')" @click.native="step(1)"></el-step>
      <!-- <el-step :title="$t('sampleSql')" @click.native="step(2)"></el-step> -->
      <el-step :title="$t('dimensions')" @click.native="step(2)"></el-step>
      <el-step :title="$t('measures')" @click.native="step(3)"></el-step>
      <el-step :title="$t('refreshSetting')" @click.native="step(4)"></el-step>
      <el-step :title="$t('tableIndex')" @click.native="step(5)"></el-step>
      <el-step :title="$t('AdvancedSetting')" @click.native="step(6)"></el-step>
      <el-step :title="$t('overview')" @click.native="step(7)"></el-step>
    </el-steps>
  </div>

  <div class="line margin-l-r"></div>
  <div class="ksd-mt-10 ksd-mb-10" id="cube-main">
  <info ref="infoForm" v-if="activeStep===1" :cubeDesc="cubeDetail" :cubeInstance="extraoption.cubeInstance" :modelDesc="modelDetail" :isEdit="isEdit" :sampleSql="sampleSql" :healthStatus="healthStatus"></info>
  <!-- <sample_sql v-if="activeStep===2" :cubeDesc="cubeDetail" :isEdit="isEdit" :sampleSql="sampleSQL"></sample_sql> -->
  <dimensions v-if="activeStep===2" :cubeDesc="cubeDetail" :cubeInstance="extraoption.cubeInstance" :modelDesc="modelDetail" :isEdit="isEdit" :sampleSql="sampleSql" :oldData="oldData" :healthStatus="healthStatus"></dimensions>
  <measures v-if="activeStep===3" :cubeDesc="cubeDetail" :cubeInstance="extraoption.cubeInstance" :modelDesc="modelDetail" :isEdit="isEdit" :sampleSql="sampleSql"  :oldData="oldData"></measures>
  <refresh_setting v-if="activeStep===4" :cubeDesc="cubeDetail" :cubeInstance="extraoption.cubeInstance" :isEdit="isEdit" :modelDesc="modelDetail" :scheduler="scheduler"></refresh_setting>
  <table_index v-if="activeStep===5" :cubeDesc="cubeDetail" :cubeInstance="extraoption.cubeInstance" :isEdit="isEdit" :modelDesc="modelDetail"  :rawTable="rawTable"></table_index>
  <configuration_overwrites v-if="activeStep===6" :cubeDesc="cubeDetail" :cubeInstance="extraoption.cubeInstance" :isEdit="isEdit"></configuration_overwrites>
  <overview v-if="activeStep===7" :cubeDesc="cubeDetail" :cubeInstance="extraoption.cubeInstance" :modelDesc="modelDetail"></overview>
  </div>

<!-- <el-button  type="primary"  @click.native="saveDraft(true)">Draft</el-button> -->
  <el-button class="button_right" type="primary" v-if="activeStep !== 7" @click.native="next">{{$t('next')}}<i class="el-icon-arrow-right el-icon--right"></i></el-button>
  <el-button class="button_right" :loading="cubeSaving" type="primary" v-if="activeStep === 7" @click.native="saveOrUpdate">{{$t('save')}}</el-button>
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
import sampleSql from './sample_sql_edit'
import dimensions from './dimensions_edit'
import measures from './measures_edit'
import refreshSetting from './refresh_setting_edit'
import tableIndex from './table_index_edit'
import configurationOverwrites from './configuration_overwrites_edit'
import overview from './overview_edit'
import json from '../json'
import { modelHealthStatus, IntegerType } from '../../../config/index'
import { removeNameSpace, getNameSpace, objectClone } from '../../../util/index'
import { handleSuccess, handleError, kapConfirm, loadBaseEncodings } from '../../../util/business'
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
      hisSchedulerStr: '',
      renderCubeFirst: false,
      aliasMap: {},
      index: 0,
      // 定时保存配置
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
      oldData: {
        oldDimensions: [],
        oldMeasures: [],
        oldColumnFamily: []
      },
      sampleSql: {
        sqlString: '',
        sqlCount: 0,
        result: []
      },
      healthStatus: {
        status: 'NONE',
        progress: 0,
        messages: []
      },
      scheduler: {
        cubeName: '',
        desc: {
          partition_interval: 0,
          repeat_count: 65535,
          repeat_interval: 0,
          startTime: 0,
          scheduled_run_time: 0
        }
      },
      selected_project: this.extraoption.project,
      wizardSteps: [
        {title: 'checkCubeInfo', isComplete: false},
        // {title: 'checkSampleSql', isComplete: false},
        {title: 'checkDimensions', isComplete: false},
        {title: 'checkMeasures', isComplete: false},
        {title: 'checkRefreshSetting', isComplete: false},
        {title: 'checkTableIndex', isComplete: false},
        {title: 'checkConfigurationOverwrite', isComplete: false}
      ],
      showErrorVisible: false,
      errorMsg: ''
    }
  },
  components: {
    'info': info,
    'sample_sql': sampleSql,
    'dimensions': dimensions,
    'measures': measures,
    'refresh_setting': refreshSetting,
    'table_index': tableIndex,
    'configuration_overwrites': configurationOverwrites,
    'overview': overview,
    'json': json
  },
  methods: {
    ...mapActions({
      checkCubeNameAvailability: 'CHECK_CUBE_NAME_AVAILABILITY',
      loadCubeDesc: 'LOAD_CUBE_DESC',
      updateCube: 'UPDATE_CUBE',
      saveCube: 'SAVE_CUBE',
      loadModelInfo: 'LOAD_MODEL_INFO',
      saveSampleSql: 'SAVE_SAMPLE_SQL',
      loadRawTable: 'GET_RAW_TABLE',
      updateRawTable: 'UPDATE_RAW_TABLE',
      saveRawTable: 'SAVE_RAW_TABLE',
      deleteRawTable: 'DELETE_RAW_TABLE',
      getScheduler: 'GET_SCHEDULER',
      updateScheduler: 'UPDATE_SCHEDULER',
      deleteScheduler: 'DELETE_SCHEDULER',
      draftCube: 'DRAFT_CUBE',
      loadDataSourceByProject: 'LOAD_DATASOURCE',
      getSql: 'GET_SAMPLE_SQL',
      getModelDiagnose: 'DIAGNOSE'
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
      let _this = this
      switch (index) {
        case 1:
          return _this.checkCubeInfo()
        // case 2:
        //   return _this.checkSampleSql()
        case 2:
          return _this.checkDimensions()
        case 3:
          return _this.checkMeasures()
        case 4:
          return _this.checkRefreshSetting()
        case 5:
          return _this.checkTableIndex()
        case 6:
          return _this.checkConfigurationOverwrite()
        default:
          return true
      }
    },
    checkCubeInfo: function () {
      let nameUsed = false
      if (!this.isEdit) {
        this.checkCubeNameAvailability({cubeName: this.cubeDetail.name}).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            if (!data) {
              this.$message({
                showClose: true,
                duration: 0,
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
      }
      return true
    },
    checkDimensions: function () {
      let _this = this
      if (_this.cubeDetail.dimensions.length <= 0) {
        this.$message({
          showClose: true,
          duration: 0,
          message: _this.$t('checkDimensions'),
          type: 'error'
        })
        return false
      }
      for (let j = 0; j < _this.cubeDetail.aggregation_groups.length; j++) {
        if (!_this.cubeDetail.aggregation_groups[j] || !_this.cubeDetail.aggregation_groups[j].includes || _this.cubeDetail.aggregation_groups[j].includes.length === 0) {
          this.$message({
            showClose: true,
            duration: 0,
            message: _this.$t('checkAggGroup'),
            type: 'error'
          })
          return false
        }
      }
      let shardRowkeyList = []
      for (let i = 0; i < _this.cubeDetail.rowkey.rowkey_columns.length; i++) {
        if (_this.cubeDetail.rowkey.rowkey_columns[i].isShardBy === true) {
          shardRowkeyList.push(_this.cubeDetail.rowkey.rowkey_columns[i].column)
        }
        if (_this.cubeDetail.rowkey.rowkey_columns[i].encoding.substr(0, 3) === 'int' && (_this.cubeDetail.rowkey.rowkey_columns[i].encoding.substr(4) < 1 || _this.cubeDetail.rowkey.rowkey_columns[i].encoding.substr(4) > 8)) {
          this.$message({
            showClose: true,
            duration: 0,
            message: _this.$t('checkRowkeyInt'),
            type: 'error'
          })
          return false
        }
        if (_this.cubeDetail.rowkey.rowkey_columns[i].encoding.substr(0, 12) === 'fixed_length' && _this.cubeDetail.rowkey.rowkey_columns[i].encoding.substr(13) === '') {
          this.$message({
            showClose: true,
            duration: 0,
            message: _this.$t('fixedLengthTip'),
            type: 'error'
          })
          return false
        }
        if (_this.cubeDetail.rowkey.rowkey_columns[i].encoding.substr(0, 16) === 'fixed_length_hex' && _this.cubeDetail.rowkey.rowkey_columns[i].encoding.substr(17) === '') {
          this.$message({
            showClose: true,
            duration: 0,
            message: _this.$t('fixedLengthHexTip'),
            type: 'error'
          })
          return false
        }
      }
      if (shardRowkeyList.length > 1) {
        this.$message({
          duration: 0,  // 不自动关掉提示
          showClose: true,    // 给提示框增加一个关闭按钮
          message: _this.$t('checkRowkeyShard'),
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
            duration: 0,  // 不自动关掉提示
            showClose: true,    // 给提示框增加一个关闭按钮
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
          duration: 0,
          message: this.$t('checkMeasuresCount'),
          type: 'error'
        })
        return false
      }
      let cfMeasures = []
      this.cubeDetail.hbase_mapping.column_family.forEach((cf) => {
        cf.columns[0].measure_refs.forEach((measure, index) => {
          cfMeasures.push(measure)
        })
      })
      if (cfMeasures.length !== this.cubeDetail.measures.length) {
        this.$message({
          showClose: true,
          duration: 0,
          message: this.$t('checkColumnFamily'),
          type: 'error'
        })
        return false
      }
      for (let j = 0; j < this.cubeDetail.hbase_mapping.column_family.length; j++) {
        if (this.cubeDetail.hbase_mapping.column_family[j].columns[0].measure_refs.length === 0) {
          this.$message({
            showClose: true,
            duration: 0,
            message: this.$t('checkColumnFamilyNull'),
            type: 'error'
          })
          return false
        }
      }
      return true
    },
    checkRefreshSetting: function () {
      if (this.$store.state.cube.cubeSchedulerIsSetting) {
        if (this.scheduler.desc.scheduled_run_time === 0 || this.scheduler.desc.partition_interval === 0) {
          this.$message(this.$t('unsetScheduler'))
          return false
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
        if (curColumn.encoding.substr(0, 7) === 'integer' && (curColumn.encoding.substr(8) < 1 || curColumn.encoding.substr(8) > 8)) {
          this.$message({
            showClose: true,
            duration: 0,
            message: this.$t('checkRowkeyInt'),
            type: 'error'
          })
          return false
        }
        if (curColumn.encoding.substr(0, 12) === 'fixed_length' && curColumn.encoding.substr(13) === '') {
          this.$message({
            showClose: true,
            duration: 0,
            message: this.$t('fixedLengthTip'),
            type: 'error'
          })
          return false
        }
        if (curColumn.encoding.substr(0, 16) === 'fixed_length_hex' && curColumn.encoding.substr(17) === '') {
          this.$message({
            showClose: true,
            duration: 0,
            message: this.$t('fixedLengthHexTip'),
            type: 'error'
          })
          return false
        }
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
          duration: 0,
          message: this.$t('fuzzyTip'),
          type: 'error'
        })
        return false
      }
      if (setTypeError) {
        this.$message({
          showClose: true,
          duration: 0,
          message: this.$t('rawtableSortedWidthDate'),
          type: 'error'
        })
        return false
      }
      if (shardCount > 1) {
        this.$message({
          showClose: true,
          duration: 0,
          message: this.$t('shardCountError'),
          type: 'error'
        })
        return false
      }
      if (sortedCount === 0) {
        this.$message({
          showClose: true,
          duration: 0,
          message: this.$t('rawtableSetSorted'),
          type: 'error'
        })
        return false
      } else {
        return true
      }
    },
    checkConfigurationOverwrite: function () {
      for (var key in this.cubeDetail.override_kylin_properties) {
        if (key === '') {
          this.$message({
            showClose: true,
            duration: 0,
            message: this.$t('checkCOKey'),
            type: 'error'
          })
          return false
        }
        if (this.cubeDetail.override_kylin_properties[key] === '') {
          this.$message({
            showClose: true,
            message: this.$t('checkCOValue'),
            type: 'error'
          })
          return false
        }
      }
      return true
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
            duration: 0,  // 不自动关掉提示
            showClose: true,    // 给提示框增加一个关闭按钮
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
      // draft 的情况不需要加删除逻辑
      // if (cloneCubeDescData && cloneCubeDescData.engine_type && (+cloneCubeDescData.engine_type === 100 || +cloneCubeDescData.engine_type === 99)) {
      //   delete cloneCubeDescData.hbase_mapping
      // }
      // cloneCubeDescData.partition_date_start = transToUTCMs(cloneCubeDescData.partition_date_start)
      saveData.cubeDescData = JSON.stringify(cloneCubeDescData)
      if (this.rawTable.tableDetail.columns && this.rawTable.tableDetail.columns.length) {
        saveData.rawTableDescData = JSON.stringify(this.rawTable.tableDetail)
      }
      // if (this.$store.state.cube.cubeSchedulerIsSetting) {
      var schedulerObj = this.scheduler.desc
      schedulerObj.name = this.cubeDetail.name
      schedulerObj.project = this.selected_project
      // 将scheduler 的自动构建触发时间设置转换成UTC
      var schedulerObjClone = JSON.parse(JSON.stringify(schedulerObj))
      schedulerObjClone.repeat_interval = schedulerObjClone.partition_interval
      schedulerObjClone.enabled = this.$store.state.cube.cubeSchedulerIsSetting
      // schedulerObjClone.scheduled_run_time = transToUTCMs(schedulerObjClone.scheduled_run_time)
      saveData.schedulerJobData = JSON.stringify(schedulerObjClone)
      // }
      this.draftCube(saveData).then((res) => {
        this.cubeDraftSaving = false
        handleSuccess(res, (data, code, status, msg) => {
          try {
            var cubeData = JSON.parse(data.cubeDescData)
            this.cubeDetail.uuid = cubeData.uuid
            this.cubeDetail.last_modified = cubeData.last_modified
            // this.cubeDetail.status = cubeData.status
          } catch (e) {
          }
          try {
            var rawTableData = JSON.parse(data.rawTableDescData)
            this.rawTable.tableDetail.uuid = rawTableData.uuid
            this.rawTable.tableDetail.last_modified = rawTableData.last_modified
            // this.rawTable.tableDetail.status = rawTableData.status
          } catch (e) {
          }
          // this.$message({
          //   type: 'success',
          //   duration: 3000,
          //   message: '已经自动为您保存为草稿!'
          // })
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
      delete newObj.uuid
      return JSON.stringify(newObj)
    },
    checkHasChanged () {
      var filterCubeMetaStr = this.filterUnCheckObject(this.cubeDetail)
      var filterRawTableStr = this.filterUnCheckObject(this.rawTable.tableDetail)
      var filterSchedulerStr = this.scheduler.desc
      if (this.renderCubeFirst) {
        this.renderCubeFirst = false
        this.hisCubeMetaStr = filterCubeMetaStr
        this.hisRawTableStr = filterRawTableStr
        this.hisSchedulerStr = filterSchedulerStr
        return false
      } else {
        if (this.hisCubeMetaStr === filterCubeMetaStr && this.hisRawTableStr === filterRawTableStr && this.hisSchedulerStr === filterSchedulerStr) {
          return false
        }
      }
      this.hisCubeMetaStr = filterCubeMetaStr
      this.hisRawTableStr = filterRawTableStr
      this.hisSchedulerStr = filterSchedulerStr
      return true
    },
    saveCube () {
      if (this.cubeDraftSaving) {
        this.$message({
          duration: 0,  // 不自动关掉提示
          showClose: true,    // 给提示框增加一个关闭按钮
          type: 'warning',
          message: this.$t('kylinLang.common.saveDraft')
        })
        return
      }
      this.cubeSaving = true
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
      // draft 的情况不需要加删除逻辑
      if (cloneCubeDescData && cloneCubeDescData.engine_type && (+cloneCubeDescData.engine_type === 100 || +cloneCubeDescData.engine_type === 99)) {
        delete cloneCubeDescData.hbase_mapping
      }
      // cloneCubeDescData.partition_date_start = transToUTCMs(cloneCubeDescData.partition_date_start)
      saveData.cubeDescData = JSON.stringify(cloneCubeDescData)
      if (this.rawTable.tableDetail.columns && this.rawTable.tableDetail.columns.length) {
        saveData.rawTableDescData = JSON.stringify(this.rawTable.tableDetail)
      }
      // if (this.$store.state.cube.cubeSchedulerIsSetting) {
      var schedulerObj = this.scheduler.desc
      schedulerObj.name = this.cubeDetail.name
      schedulerObj.project = this.selected_project
      // 将scheduler 的自动构建触发时间设置转换成UTC
      var schedulerObjClone = JSON.parse(JSON.stringify(schedulerObj))
      schedulerObjClone.enabled = this.$store.state.cube.cubeSchedulerIsSetting
      schedulerObjClone.repeat_interval = schedulerObjClone.partition_interval
      // schedulerObjClone.scheduled_run_time = transToUTCMs(schedulerObjClone.scheduled_run_time)
      saveData.schedulerJobData = JSON.stringify(schedulerObjClone)
      // }
      // if (this.rawTable.tableDetail.columns && this.rawTable.tableDetail.columns.length) {
      //   saveData.rawTableDescData = JSON.stringify(this.rawTable.tableDetail)
      // }
      // var schedulerObj = this.scheduler.desc
      // schedulerObj.name = this.cubeDetail.name
      // schedulerObj.project = this.selected_project
      // saveData.schedulerJobData = JSON.stringify(schedulerObj)
      this.updateCube(saveData).then((res) => {
        this.cubeSaving = false
        handleSuccess(res, (data, code, status, msg) => {
          this.$message({
            type: 'success',
            duration: 3000,
            message: this.$t('kylinLang.common.saveSuccess')
          })
          this.$emit('reload', 'cubeList')
          this.$emit('removetabs', 'cube' + this.extraoption.cubeName, 'Overview')
        })
      }, (res) => {
        this.cubeSaving = false
        handleError(res)
      })
    },
    saveOrUpdate: function () {
      kapConfirm(this.$t('kylinLang.cube.saveCubeTip')).then(() => {
        this.saveCube()
      })
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
        override_kylin_properties: {
          'kap.smart.conf.aggGroup.strategy': 'auto'
        }
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
      return +CubeEng
    },
    getStorageEng: function () {
      let StorageEng = this.getProperty('kylin.storage.default').trim()
      if (!StorageEng) {
        return 2
      }
      return +StorageEng
    },
    loadCubeDetail: function () {
      var _this = this
      this.loadCubeDesc({cubeName: this.extraoption.cubeName, project: this.extraoption.project}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.cubeDetail = data.cube || data.draft
          if (data.cube && data.draft) {
            kapConfirm(this.$t('kylinLang.common.checkDraft'), {
              confirmButtonText: 'OK',
              cancelButtonText: 'NO'
            }).then(() => {
              this.cubeDetail = data.draft
              loadRowTable(true)
              loadScheduler(true)
            }).catch(() => {
              this.cubeDetail = data.cube
              this.cubeDetail.status = this.extraoption.cubeStatus
              loadRowTable(false)
              loadScheduler(false)
            })
          } else {
            if (data.cube) {
              this.cubeDetail.status = this.extraoption.cubeStatus
              loadRowTable(false)
              loadScheduler(false)
            } else {
              loadScheduler(true)
              loadRowTable(true)
            }
          }
          if (!this.cubeDetail.override_kylin_properties['kap.smart.conf.aggGroup.strategy']) {
            this.$set(this.cubeDetail.override_kylin_properties, 'kap.smart.conf.aggGroup.strategy', 'auto')
          }
          if (this.cubeDetail.override_kylin_properties['kap.smart.conf.aggGroup.strategy'] === 'mixed') {
            this.cubeDetail.override_kylin_properties['kap.smart.conf.aggGroup.strategy'] = 'auto'
          }
          this.oldData.oldDimensions = objectClone(this.cubeDetail.dimensions)
          this.oldData.oldMeasures = objectClone(this.cubeDetail.measures)
          this.oldData.oldColumnFamily = objectClone(this.cubeDetail.hbase_mapping.column_family)
          function loadRowTable (isDraft) {
            _this.$store.state.cube.cubeRowTableIsSetting = false
            _this.loadRawTable({cubeName: _this.extraoption.cubeName, project: _this.selected_project}).then((res) => {
              handleSuccess(res, (data, code, status, msg) => {
                var rawtableData = isDraft ? data.draft : data.rawTable
                if (rawtableData) {
                  _this.$set(_this.rawTable, 'tableDetail', rawtableData)
                  _this.$store.state.cube.cubeRowTableIsSetting = true
                }
              })
            })
          }
          function loadScheduler (isDraft) {
            _this.$store.state.cube.cubeSchedulerIsSetting = false
            _this.getScheduler({cubeName: _this.extraoption.cubeName, project: _this.selected_project}).then((res) => {
              handleSuccess(res, (data, code, status, msg) => {
                var schedulerData = isDraft ? data.draft : data.schedulerJob
                // this.initRepeatInterval(schedulerData)
                if (schedulerData) {
                  _this.scheduler.desc.scheduled_run_time = schedulerData.scheduled_run_time
                  // this.scheduledRunTime = transToUtcTimeFormat(this.scheduler.desc.scheduled_run_time)
                  _this.scheduler.desc.partition_interval = schedulerData.partition_interval
                  if (schedulerData.enabled) {
                    _this.$store.state.cube.cubeSchedulerIsSetting = true
                  }
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
    getModelHelthInfo (project, modelName) {
      this.getModelDiagnose({
        project: project,
        modelName: modelName
      }).then((res) => {
        handleSuccess(res, (data) => {
          this.healthStatus.status = data.heathStatus
          this.healthStatus.progress = data.progress
          this.healthStatus.messages = data.messages && data.messages.length ? data.messages.map((x) => {
            return x.replace(/\r\n/g, '<br/>')
          }) : [modelHealthStatus[data.heathStatus].message]
        })
      })
    },
    loadSql () {
      this.getSql(this.cubeDetail.name).then((res) => {
        handleSuccess(res, (data) => {
          if (data.sqls) {
            this.sampleSql.sqlCount = data.sqls.length
            this.sampleSql.result = data.results
            this.sampleSql.sqlString = data.sqls.length > 0 ? data.sqls.join(';\r\n') + ';' : ''
          }
        })
      })
    }
  },
  created () {
    this.createNewCube()
    this.$store.state.cube.cubeSchedulerIsSetting = false
    this.$store.state.cube.cubeRowTableIsSetting = false
    if (this.isEdit) {
      this.loadCubeDetail()
    }
    if (this.modelDetail) {
      this.loadSql()
      this.getModelHelthInfo(this.selected_project, this.extraoption.modelName)
    }
    this.loadDataSourceByProject({project: this.selected_project, isExt: true}).then(() => {
      this.loadModelInfo({modelName: this.extraoption.modelName, project: this.selected_project}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.renderCubeFirst = true
          this.modelDetail = data.model
          this.getTables()
          var rawtableBaseData = this.getModelColumnsDataForRawtable()
          this.cacheRawTableBaseData({project: this.selected_project, modelName: this.extraoption.modelName, data: rawtableBaseData})
          if (!this.isEdit) {
            let datatype = this.modelDetail.partition_desc.partition_date_column && this.modelDetail.columnsDetail[this.modelDetail.partition_desc.partition_date_column].datatype
            if (!this.modelDetail.partition_desc.partition_date_format && IntegerType.indexOf(datatype) >= 0) {
              this.cubeDetail.auto_merge_time_ranges.splice(0, this.cubeDetail.auto_merge_time_ranges.length)
            }
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
    'en': {cubeInfo: 'Cube Info', sampleSql: 'Sample SQL', dimensions: 'Dimensions', measures: 'Measures', refreshSetting: 'Refresh Setting', tableIndex: 'Table Index', AdvancedSetting: 'Advanced Setting', overview: 'Overview', prev: 'Prev', next: 'Next', save: 'Save', checkCubeNamePartOne: 'The Cube name [ ', checkCubeNamePartTwo: ' ] already exists!', checkDimensions: 'Please select at least one dimension.', checkAggGroup: 'Each aggregation group can\'t be empty.', checkMeasuresCount: '[COUNT] measure is required.', checkRowkeyInt: 'Integer encoding column length should between 1 and 8.', checkRowkeyShard: 'At most one \'shard by\' column is allowed.', checkColumnFamily: 'All measures need to be assigned to column family.', checkColumnFamilyNull: 'Each column family can\'t not be empty.', checkCOKey: 'Property name is required.', checkCOValue: 'Property value is required.', rawtableSetSorted: 'Please set at least one column with Sort By value of "true".', rawtableSortedWidthDate: 'The first "sorted" column should be a column with encoding "integer", "time" or "date".', rawtableSingleSorted: 'Only one column is allowed to set with an index value of "sorted".', errorMsg: 'Error Message', shardCountError: 'Max Shard By column number is 1.', unsetScheduler: 'Please complete the Scheduler setting.', fuzzyTip: 'Fuzzy index can be applied to string(varchar) type column only.', checkCountDistinctPartOne: '[', checkCountDistinctPartTwo: '］is currently not supported as both a cube dimension and a count distinct(bitmap) measure.', checkCountDistinctPartThree: 'Please apply count distinct(hllc) instead or remove [', checkCountDistinctPartFour: '] from cube dimension list.', returnTypeNullPartOne: 'Measure[', returnTypeNullPartTwo: ']\'s return type is null. Please edit it to fill the blank.', strategyTip1: 'On the "', strategyTip2: '", cube optimizer can barely work for model check/SQL pattens being uncompleted. Are you sure to continue?', dataOriented: 'Data Oriented', mix: 'Mix', businessOriented: 'Business Oriented', fixedLengthTip: 'The length parameter of Fixed Length encoding is required.', fixedLengthHexTip: 'The length parameter of Fixed Length Hex encoding is required.'},
    'zh-cn': {cubeInfo: 'Cube信息', sampleSql: '查询样例', dimensions: '维度', measures: '度量', refreshSetting: '刷新设置', tableIndex: '表索引', AdvancedSetting: '高级设置', overview: '概览', prev: '上一步', next: '下一步', save: '保存', checkCubeNamePartOne: '名为 [ ', checkCubeNamePartTwo: '] 的Cube已经存在。', checkDimensions: '维度不能为空。', checkAggGroup: '任意聚合组不能为空。', checkMeasuresCount: '[COUNT] 度量是必须的。', checkRowkeyInt: '编码为int的列的长度应该在1至8之间。', checkRowkeyShard: '最多只允许一个\'shard by\'的列。', checkColumnFamily: '所有度量都应被分配到列簇中。', checkColumnFamilyNull: '任一列簇不能为空!', checkCOKey: '属性名不能为空。', checkCOValue: '属性值不能为空。', rawtableSetSorted: '至少设置一个列的Sort By值为"true"。', rawtableSortedWidthDate: '第一个sorted列应为编码为integer、date或time的列。', rawtableSingleSorted: '只允许设置一个列的index的值为sorted。', errorMsg: '错误信息', shardCountError: 'Shard By最多可以设置一列。', unsetScheduler: 'Scheduler参数设置不完整。', fuzzyTip: '模糊(fuzzy)索引只支持应用于string（varchar）类型数据。', checkCountDistinctPartOne: '当前版本中，[', checkCountDistinctPartTwo: ']无法既做cube维度也作为count distinct(bitmap) 的度量参数。', checkCountDistinctPartThree: '请使用count distinct(hllc) 替换该度量，或从cube维度列表中删除[', checkCountDistinctPartFour: ']。', returnTypeNullPartOne: '度量[', returnTypeNullPartTwo: ']的返回类型为空，请重新设置该度量。', strategyTip1: '当前优化偏好“', strategyTip2: '”下，模型检测／输入SQL尚未完成，后续优化与推荐可能无法完成。您确定要继续下一步吗？', dataOriented: '模型优先', mix: '综合', businessOriented: '业务优先', fixedLengthTip: 'Fixed Length Hex编码时需要长度参数。', fixedLengthHexTip: 'Fixed Length Hex编码时需要长度参数。'}
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
  }
  #cube-main{
    *{
      font-size: 12px;
    }
  }
</style>
