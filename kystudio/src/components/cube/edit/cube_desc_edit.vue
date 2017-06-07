<template>
<div class="paddingbox">
  <el-steps :active="activeStep"  finish-status="finish" process-status="wait" center align-center style="width:96%;margin:0 auto">
    <el-step :title="$t('cubeInfo')" @click.native="step(1)"></el-step>
    <el-step :title="$t('sampleSql')" @click.native="step(2)"></el-step>
    <el-step :title="$t('dimensions')" @click.native="step(3)"></el-step>
    <el-step :title="$t('measures')" @click.native="step(4)"></el-step>
    <el-step :title="$t('refreshSetting')" @click.native="step(5)"></el-step>
    <el-step :title="$t('tableIndex')" @click.native="step(6)"></el-step>
    <el-step :title="$t('AdvancedSetting')" @click.native="step(7)"></el-step>
    <el-step :title="$t('overview')" @click.native="step(8)"></el-step>
  </el-steps>
  <div class="ksd-mt-10 ksd-mb-10">
  <info v-if="activeStep===1" :cubeDesc="cubeDetail" :modelDesc="modelDetail" :isEdit="isEdit"></info>
  <sample_sql v-if="activeStep===2" :cubeDesc="cubeDetail" :isEdit="isEdit" :sampleSql="sampleSQL"></sample_sql>
  <dimensions v-if="activeStep===3" :cubeDesc="cubeDetail" :modelDesc="modelDetail" :isEdit="isEdit"></dimensions>
  <measures v-if="activeStep===4" :cubeDesc="cubeDetail" :modelDesc="modelDetail" :isEdit="isEdit"></measures>
  <refresh_setting v-if="activeStep===5" :cubeDesc="cubeDetail" :isEdit="isEdit" :modelDesc="modelDetail" :scheduler="scheduler"></refresh_setting>
  <table_index v-if="activeStep===6" :cubeDesc="cubeDetail" :isEdit="isEdit" :modelDesc="modelDetail"  :rawTable="rawTable"></table_index>
  <configuration_overwrites v-if="activeStep===7" :cubeDesc="cubeDetail" :isEdit="isEdit"></configuration_overwrites>
  <overview v-if="activeStep===8" :cubeDesc="cubeDetail" :modelDesc="modelDetail"></overview>
  </div>
  
<!-- <el-button  type="primary"  @click.native="saveDraft(true)">Draft</el-button> -->
  <el-button class="button_right" type="primary" v-if="activeStep !== 8" @click.native="next">{{$t('next')}}<i class="el-icon-arrow-right el-icon--right"></i></el-button>
  <el-button class="button_right" :loading="cubeSaving" type="primary" v-if="activeStep === 8" @click.native="saveOrUpdate">{{$t('save')}}</el-button>
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
import { mapActions } from 'vuex'
import info from './info_edit'
import sampleSql from './sample_sql_edit'
import dimensions from './dimensions_edit'
import measures from './measures_edit'
import refreshSetting from './refresh_setting_edit'
import tableIndex from './table_index_edit'
import configurationOverwrites from './configuration_overwrites_edit'
import overview from './overview_edit'
import json from '../json'
import { removeNameSpace, objectClone } from '../../../util/index'
import { handleSuccess, handleError } from '../../../util/business'
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
      sampleSQL: {sqlString: ''},
      scheduler: {
        cubeName: '',
        desc: {
          partition_interval: 86400000,
          repeat_count: 65535,
          repeat_interval: 0,
          startTime: 0,
          scheduled_run_time: 0
        }
      },
      selected_project: this.extraoption.project,
      wizardSteps: [
        {title: 'checkCubeInfo', isComplete: false},
        {title: 'checkSampleSql', isComplete: false},
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
      loadDataSourceByProject: 'LOAD_DATASOURCE'
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
        case 2:
          return _this.checkSampleSql()
        case 3:
          return _this.checkDimensions()
        case 4:
          return _this.checkMeasures()
        case 5:
          return _this.checkRefreshSetting()
        case 6:
          return _this.checkTableIndex()
        case 7:
          return _this.checkConfigurationOverwrite()
        default:
          return true
      }
    },
    checkCubeInfo: function () {
      let _this = this
      let nameUsed = false
      if (!_this.isEdit) {
        this.checkCubeNameAvailability(_this.cubeDetail.name).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            if (data === false) {
              this.$message({
                showClose: true,
                duration: 3000,
                message: _this.$t('checkCubeNamePartOne') + this.cubeDetail.name.toUpperCase() + _this.$t('checkCubeNamePartTwo'),
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
    checkSampleSql: function () {
      console.log(880)
      let _this = this
      if (_this.sampleSQL.sqlString !== '') {
        _this.saveSampleSql({modelName: _this.modelDetail.name, cubeName: _this.cubeDetail.name, sqls: _this.sampleSQL.sqlString.split(/\r?\n/)})
      }
      return true
    },
    checkDimensions: function () {
      let _this = this
      if (_this.cubeDetail.dimensions.length <= 0) {
        this.$message({
          showClose: true,
          duration: 3000,
          message: _this.$t('checkDimensions'),
          type: 'error'
        })
        return false
      }
      for (let j = 0; j < _this.cubeDetail.aggregation_groups.length; j++) {
        if (!_this.cubeDetail.aggregation_groups[j] || !_this.cubeDetail.aggregation_groups[j].includes || _this.cubeDetail.aggregation_groups[j].includes.length === 0) {
          this.$message({
            showClose: true,
            duration: 3000,
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
            duration: 3000,
            message: _this.$t('checkRowkeyInt'),
            type: 'error'
          })
          return false
        }
      }
      if (shardRowkeyList.length > 1) {
        this.$message({
          showClose: true,
          message: _this.$t('checkRowkeyShard'),
          type: 'error'
        })
        return false
      }
      return true
    },
    checkMeasures: function () {
      let _this = this
      let existCountExpression = false
      for (let i = 0; i < _this.cubeDetail.measures.length; i++) {
        if (_this.cubeDetail.measures[i].function.expression === 'COUNT') {
          existCountExpression = true
        }
      }
      if (!existCountExpression) {
        this.$message({
          showClose: true,
          duration: 3000,
          message: _this.$t('checkMeasuresCount'),
          type: 'error'
        })
        return false
      }
      let cfMeasures = []
      _this.cubeDetail.hbase_mapping.column_family.forEach(function (cf) {
        cf.columns[0].measure_refs.forEach(function (measure, index) {
          cfMeasures.push(measure)
        })
      })
      if (cfMeasures.length !== _this.cubeDetail.measures.length) {
        this.$message({
          showClose: true,
          duration: 3000,
          message: _this.$t('checkColumnFamily'),
          type: 'error'
        })
        return false
      }
      for (let j = 0; j < _this.cubeDetail.hbase_mapping.column_family.length; j++) {
        if (_this.cubeDetail.hbase_mapping.column_family[j].columns[0].measure_refs.length === 0) {
          this.$message({
            showClose: true,
            duration: 3000,
            message: _this.$t('checkColumnFamilyNull'),
            type: 'error'
          })
          return false
        }
      }
      return true
    },
    checkRefreshSetting: function () {
      return true
    },
    checkTableIndex: function () {
      let _this = this
      if (_this.rawTable.tableDetail.columns.length === 0) {
        return true
      }
      let sortedCount = 0
      let shardCount = 0
      let setTypeError = false
      for (let i = 0; i < this.rawTable.tableDetail.columns.length; i++) {
        if (this.rawTable.tableDetail.columns[i].is_sortby === true) {
          let _encoding = _this.getEncoding(this.rawTable.tableDetail.columns[i].encoding)
          if (['date', 'time', 'integer'].indexOf(_encoding) < 0) {
            if (sortedCount === 0) {
              setTypeError = true
            }
          }
          sortedCount++
        }
        if (this.rawTable.tableDetail.columns[i].is_shardby === true) {
          shardCount++
        }
      }
      if (setTypeError) {
        this.$message({
          showClose: true,
          duration: 3000,
          message: _this.$t('rawtableSortedWidthDate'),
          type: 'error'
        })
        return false
      }
      if (shardCount > 1) {
        this.$message({
          showClose: true,
          duration: 3000,
          message: _this.$t('shardCountError'),
          type: 'error'
        })
        return false
      }
      if (sortedCount === 0) {
        this.$message({
          showClose: true,
          duration: 3000,
          message: _this.$t('rawtableSetSorted'),
          type: 'error'
        })
        return false
      } else {
        return true
      }
    },
    checkConfigurationOverwrite: function () {
      let _this = this
      for (var key in _this.cubeDetail.override_kylin_properties) {
        if (key === '') {
          this.$message({
            showClose: true,
            duration: 3000,
            message: _this.$t('checkCOKey'),
            type: 'error'
          })
          return false
        }
        if (_this.cubeDetail.override_kylin_properties[key] === '') {
          this.$message({
            showClose: true,
            message: _this.$t('checkCOValue'),
            type: 'error'
          })
          return false
        }
      }
      return true
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
      // cloneCubeDescData.partition_date_start = transToUTCMs(cloneCubeDescData.partition_date_start)
      saveData.cubeDescData = JSON.stringify(cloneCubeDescData)
      if (this.rawTable.tableDetail.columns && this.rawTable.tableDetail.columns.length) {
        saveData.rawTableDescData = JSON.stringify(this.rawTable.tableDetail)
      }
      var schedulerObj = this.scheduler.desc
      schedulerObj.name = this.cubeDetail.name
      schedulerObj.project = this.selected_project
      // 将scheduler 的自动构建触发时间设置转换成UTC
      var schedulerObjClone = JSON.parse(JSON.stringify(schedulerObj))
      // console.log(schedulerObjClone.scheduled_run_time, 9911)
      schedulerObjClone.repeat_interval = schedulerObjClone.partition_interval
      // schedulerObjClone.scheduled_run_time = transToUTCMs(schedulerObjClone.scheduled_run_time)
      saveData.schedulerJobData = JSON.stringify(schedulerObjClone)
      this.draftCube(saveData).then((res) => {
        this.cubeDraftSaving = false
        handleSuccess(res, (data, code, status, msg) => {
          try {
            var cubeData = JSON.parse(data.cubeDescData)
            this.cubeDetail.uuid = cubeData.uuid
            this.cubeDetail.last_modified = cubeData.last_modified
            this.cubeDetail.status = cubeData.status
          } catch (e) {
          }
          try {
            var rawTableData = JSON.parse(data.rawTableDescData)
            this.rawTable.tableDetail.uuid = rawTableData.uuid
            this.rawTable.tableDetail.last_modified = rawTableData.last_modified
            this.rawTable.tableDetail.status = rawTableData.status
          } catch (e) {
          }
          this.$message({
            type: 'success',
            duration: 3000,
            message: '已经自动为您保存为草稿!'
          })
          this.$emit('reload', 'cubeList')
        })
      }).catch((res) => {
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
      // cloneCubeDescData.partition_date_start = transToUTCMs(cloneCubeDescData.partition_date_start)
      saveData.cubeDescData = JSON.stringify(cloneCubeDescData)
      if (this.rawTable.tableDetail.columns && this.rawTable.tableDetail.columns.length) {
        saveData.rawTableDescData = JSON.stringify(this.rawTable.tableDetail)
      }
      var schedulerObj = this.scheduler.desc
      schedulerObj.name = this.cubeDetail.name
      schedulerObj.project = this.selected_project
      // 将scheduler 的自动构建触发时间设置转换成UTC
      var schedulerObjClone = JSON.parse(JSON.stringify(schedulerObj))
      schedulerObjClone.repeat_interval = schedulerObjClone.partition_interval
      // schedulerObjClone.scheduled_run_time = transToUTCMs(schedulerObjClone.scheduled_run_time)
      saveData.schedulerJobData = JSON.stringify(schedulerObjClone)
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
            message: '保存成功!'
          })
          this.$emit('reload', 'cubeList')
          this.$emit('removetabs', 'cube' + this.extraoption.cubeName)
        })
      }).catch((res) => {
        this.cubeSaving = false
        handleError(res)
      })
    },
    saveOrUpdate: function () {
      this.$confirm('确认保存Cube？', '提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        this.saveCube()
      }).catch((e) => {
      })
    },
    saveOrUpdateRawTable: function () {
      if (this.cubeDetail.engine_type === 100 || this.cubeDetail.engine_type === 99) {
        let _this = this
        _this.rawTable.tableDetail.name = _this.cubeDetail.name
        _this.rawTable.tableDetail.model_name = _this.cubeDetail.model_name
        _this.rawTable.tableDetail.engine_type = _this.cubeDetail.engine_type
        _this.rawTable.tableDetail.storage_type = _this.cubeDetail.storage_type
        _this.loadRawTable(_this.cubeDetail.name).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            if (_this.rawTable.tableDetail.columns.length > 0) {
              _this.updateRawTable({project: this.selected_project, rawTableDescData: JSON.stringify(_this.rawTable.tableDetail), rawTableName: this.cubeDetail.name}).then((res) => {
                handleSuccess(res, (data, code, status, msg) => {
                })
              }).catch((res) => {
                handleError(res, (data, code, status, msg) => {
                })
              })
            } else {
              if (_this.rawTable.needDelete) {
                _this.deleteRawTable(this.cubeDetail.name).then((res) => {
                  handleSuccess(res, (data, code, status, msg) => {
                  })
                }).catch((res) => {
                  handleError(res, (data, code, status, msg) => {
                  })
                })
              }
            }
          })
        }).catch((res) => {
          handleError(res, (data, code, status, msg) => {
            if (_this.rawTable.tableDetail.columns.length > 0) {
              _this.saveRawTable({project: this.selected_project, rawTableDescData: JSON.stringify(_this.rawTable.tableDetail)}).then((res) => {
                handleSuccess(res, (data, code, status, msg) => {
                })
              }).catch((res) => {
                handleError(res, (data, code, status, msg) => {
                })
              })
            }
          })
        })
      }
    },
    saveOrUpdateScheduler: function () {
      let _this = this
      if (_this.isEdit) {
        _this.updateScheduler(_this.scheduler).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
          })
        }).catch((res) => {
          handleError(res, (data, code, status, msg) => {
            this.$message({
              type: 'error',
              message: msg
            })
          })
        })
      } else {
        _this.saveScheduler(_this.scheduler).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
          })
        }).catch((res) => {
          handleError(res, (data, code, status, msg) => {
            this.$message({
              type: 'error',
              message: msg
            })
          })
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
      this.loadCubeDesc(this.extraoption.cubeName).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.cubeDetail = data[0]
          this.$set(this.cubeDetail, 'name', this.cubeDetail.name.replace(/_draft$/, ''))
        })
      }).catch((res) => {
        console.log(12322)
        handleError(res)
      })
    },
    getTables: function () {
      let _this = this
      let rootFactTable = removeNameSpace(this.modelDetail.fact_table)
      let factTables = []
      let lookupTables = []
      factTables.push(rootFactTable)
      _this.$set(_this.modelDetail, 'columnsDetail', {})
      _this.$store.state.datasource.dataSource[_this.selected_project].forEach(function (table) {
        if (_this.modelDetail.fact_table === table.database + '.' + table.name) {
          table.columns.forEach(function (column) {
            _this.$set(_this.modelDetail.columnsDetail, rootFactTable + '.' + column.name, {
              name: column.name,
              datatype: column.datatype,
              cardinality: table.cardinality[column.name],
              comment: column.comment})
          })
        }
      })
      _this.modelDetail.lookups.forEach(function (lookup) {
        if (lookup.kind === 'FACT') {
          if (!lookup.alias) {
            lookup['alias'] = removeNameSpace(lookup.table)
          }
          factTables.push(lookup.alias)
        } else {
          if (!lookup.alias) {
            lookup['alias'] = removeNameSpace(lookup.table)
          }
          lookupTables.push(lookup.alias)
        }
        _this.$store.state.datasource.dataSource[_this.selected_project].forEach(function (table) {
          if (lookup.table === table.database + '.' + table.name) {
            table.columns.forEach(function (column) {
              _this.$set(_this.modelDetail.columnsDetail, lookup.alias + '.' + column.name, {
                name: column.name,
                datatype: column.datatype,
                cardinality: table.cardinality[column.name],
                comment: column.comment})
            })
          }
        })
      })
      _this.$set(this.modelDetail, 'lookupTables', lookupTables)
      _this.$set(this.modelDetail, 'factTables', factTables)
    }
  },
  created () {
    this.createNewCube()
    if (this.isEdit) {
      this.loadCubeDetail()
    }
    this.loadDataSourceByProject(this.selected_project)
    this.loadModelInfo(this.extraoption.modelName).then((res) => {
      handleSuccess(res, (data, code, status, msg) => {
        this.renderCubeFirst = true
        this.modelDetail = data.model
        this.getTables()
      })
    }).catch((res) => {
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
    'en': {cubeInfo: 'Cube Info', sampleSql: 'Sample Sql', dimensions: 'Dimensions', measures: 'Measures', refreshSetting: 'Refresh Setting', tableIndex: 'Table Index', AdvancedSetting: 'Advanced Setting', overview: 'Overview', prev: 'Prev', next: 'Next', save: 'Save', checkCubeNamePartOne: 'The CUBE named [ ', checkCubeNamePartTwo: ' ] already exists!', checkDimensions: 'Dimension can\'t be null!', checkAggGroup: 'Each aggregation group can\'t be empty!', checkMeasuresCount: '[ COUNT] metric is required!', checkRowkeyInt: 'int encoding column length should between 1 and 8!', checkRowkeyShard: 'At most one \'shard by\' column is allowed!', checkColumnFamily: 'All measures need to be assigned to column family!', checkColumnFamilyNull: 'Each column family can\'t not be empty!', checkCOKey: 'Property name is required!', checkCOValue: 'Property value is required!', rawtableSetSorted: 'You must set one column with an index value of sorted! ', rawtableSortedWidthDate: 'The first column with "sorted" index must be a column with "integer", "time" or "date" encoding! ', rawtableSingleSorted: 'Only one column is allowed to set with an index value of sorted! ', errorMsg: '错误信息', shardCountError: 'Max shard by column number is 1'},
    'zh-cn': {cubeInfo: 'Cube信息', sampleSql: '查询样例', dimensions: '维度', measures: '度量', refreshSetting: '刷新配置', tableIndex: '表索引', AdvancedSetting: '高级设置', overview: '概览', prev: 'Prev', next: 'Next', save: 'Save', checkCubeNamePartOne: '名为 [ ', checkCubeNamePartTwo: '] 的CUBE已经存在!', checkDimensions: '维度不能为空!', checkAggGroup: '任意聚合组不能为空!', checkMeasuresCount: '[ COUNT] 度量是必须的!', checkRowkeyInt: '编码为int的列的长度应该在1-8之间!', checkRowkeyShard: '最多只允许一个\'shard by\'的列!', checkColumnFamily: '所有度量都需要被分配到列族中!', checkColumnFamilyNull: '任一列族不能为空!', checkCOKey: '属性名不能为空!', checkCOValue: '属性值不能为空!', rawtableSetSorted: '必须设置一个列的index的值为sorted! ', rawtableSortedWidthDate: '第一个sorted列必须是编码为integer、date或time的列', rawtableSingleSorted: '只允许设置一个列的index的值为sorted', errorMsg: '错误信息', shardCountError: 'Shard by最多可以设置一列'}
  }
}
</script>
<style scoped="">
 .button_right {
  float: right;
  margin-left:10px;
  margin-bottom: 20px;
 }
</style>
