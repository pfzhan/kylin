<template>
  <el-dialog
    :title="partitionTitle"
    width="560px"
    append-to-body
    limited-area
    :visible="isShow"
    class="model-partition-dialog"
    @close="isShow && handleClose(false)"
    :close-on-press-escape="false"
    :close-on-click-modal="false">
    <!-- <div class="ky-list-title" v-if="!(modelInstance && modelInstance.uuid) && partitionMeta.table && partitionMeta.column">{{$t('partitionSet')}}</div> -->
    <div class="partition-set ksd-mb-10" v-if="mode === 'saveModel'">
      <el-alert
        :title="$t('changeBuildTypeTips')"
        type="warning"
        :closable="false"
        class="ksd-mb-10"
        v-if="isShowWarning"
        show-icon>
      </el-alert>
      <div class="ksd-title-label">{{$t('chooseBuildType')}}</div>
      <el-select v-model="buildType" class="ksd-mtb-10" @change="handChangeBuildType" :disabled="!datasourceActions.includes('changeBuildType')">
        <el-option :label="$t('incremental')" value="incremental"></el-option>
        <el-option :label="$t('fullLoad')" value="fullLoad"></el-option>
      </el-select>
      <el-alert
        class="ksd-pt-0"
        :title="buildTips"
        type="info"
        :show-background="false"
        :closable="false"
        show-icon>
      </el-alert>
    </div>
    <el-form v-if="mode === 'saveModel'&&buildType=== 'incremental'" :model="partitionMeta" ref="partitionForm" :rules="partitionRules"  label-width="85px" label-position="top">
      <div class="ksd-title-label ksd-mb-10">{{$t('partitionSet')}}</div>
      <el-form-item :label="$t('partitionDateTable')" class="clearfix">
        <el-row :gutter="5">
          <el-col :span="12">
            <el-select :disabled="isLoadingNewRange" v-guide.partitionTable v-model="partitionMeta.table" @change="partitionTableChange" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" style="width:100%">
              <!-- <el-option :label="$t('noPartition')" value=""></el-option> -->
              <el-option :label="t.alias" :value="t.alias" v-for="t in partitionTables" :key="t.alias">{{t.alias}}</el-option>
            </el-select>
          </el-col>
        </el-row>
      </el-form-item>
      <el-form-item  :label="$t('partitionDateColumn')" v-if="partitionMeta.table">
        <el-row :gutter="5">
          <el-col :span="11" v-if="partitionMeta.table">
            <el-form-item prop="column">
              <el-select
                :disabled="isLoadingNewRange"
                v-guide.partitionColumn
                v-model="partitionMeta.column"
                :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
                filterable
                class="partition-column"
                popper-class="js_partition-column"
                style="width:100%">
              <i slot="prefix" class="el-input__icon el-icon-search" v-if="!partitionMeta.column.length"></i>
                <el-option :label="t.name" :value="t.name" v-for="t in columns" :key="t.name">
                  <el-tooltip :content="t.name" effect="dark" placement="top" :disabled="showToolTip(t.name)"><span style="float: left">{{ t.name | omit(15, '...') }}</span></el-tooltip>
                  <span class="ky-option-sub-info">{{ t.datatype.toLocaleLowerCase() }}</span>
                </el-option>
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :span="11">
            <el-select
              :disabled="isLoadingFormat"
              v-guide.partitionColumnFormat
              style="width:100%"
              v-model="partitionMeta.format"
              class="partition-column-format"
              popper-class="js_partition-column-format"
              :placeholder="$t('pleaseInputColumnFormat')">
              <el-option :label="f.label" :value="f.value" v-for="f in dateFormats" :key="f.label"></el-option>
              <!-- <el-option label="" value="" v-if="partitionMeta.column && timeDataType.indexOf(getColumnInfo(partitionMeta.column).datatype)===-1"></el-option> -->
            </el-select>
          </el-col>
          <el-col :span="1">
            <el-tooltip effect="dark" :content="$t('detectFormat')" placement="top">
              <div style="display: inline-block;">
                <el-button
                  size="medium"
                  :loading="isLoadingFormat"
                  icon="el-icon-ksd-data_range_search"
                  v-guide.getPartitionColumnFormat
                  v-if="partitionMeta.column&&$store.state.project.projectPushdownConfig"
                  @click="handleLoadFormat">
                </el-button>
              </div>
            </el-tooltip>
          </el-col>
        </el-row>
        <span v-guide.checkPartitionColumnFormatHasData style="position:absolute;width:1px; height:0" v-if="partitionMeta.format"></span>
      </el-form-item>
      <el-form-item v-if="((!modelDesc.multi_partition_desc && $store.state.project.multi_partition_enabled) || modelDesc.multi_partition_desc) && partitionMeta.table">
        <span slot="label">
          <span>{{$t('multilevelPartition')}}</span>
          <el-tooltip effect="dark" :content="$t('multilevelPartitionDesc')" placement="right">
            <i class="el-icon-ksd-what"></i>
          </el-tooltip>
        </span>
        <el-row>
          <el-col :span="11">
           <el-select
                :disabled="isLoadingNewRange"
                v-model="partitionMeta.multiPartition"
                :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
                filterable
                class="partition-multi-partition"
                popper-class="js_multi-partition"
                style="width:100%">
                <i slot="prefix" class="el-input__icon el-icon-search" v-if="!partitionMeta.multiPartition.length"></i>
                <el-option :label="$t('noPartition')" value=""></el-option>
                <el-option :label="t.name" :value="t.name" v-for="t in columns" :key="t.name">
                  <el-tooltip :content="t.name" effect="dark" placement="top" :disabled="showToolTip(t.name)"><span style="float: left">{{ t.name | omit(15, '...') }}</span></el-tooltip>
                  <span class="ky-option-sub-info">{{ t.datatype.toLocaleLowerCase() }}</span>
                </el-option>
              </el-select>
          </el-col>
        </el-row>
      </el-form-item>
    </el-form>
    <template v-if="mode === 'saveModel'">
      <div class="divide-block">
        <div class="divider"></div>
        <span v-if="isExpand" @click="toggleShowPartition">{{$t('advanceSetting')}}<i class="el-icon-ksd-more_01-copy arrow"></i></span>
        <span v-else @click="toggleShowPartition">{{$t('advanceSetting')}}<i class="el-icon-ksd-more_02 arrow"></i></span>
      </div>
      <div v-show="isExpand">
        <!-- <div class="divider"></div> -->
        <div class="ksd-title-label ksd-mb-10">
          {{$t('dataFilterCond')}}
          <el-tooltip effect="dark" :content="$t('dataFilterCondTips')" placement="right">
            <i class="el-icon-ksd-what"></i>
          </el-tooltip>
        </div>
        <el-alert
          :title="$t('filterCondTips')"
          type="warning"
          :closable="false"
          :show-background="false"
          style="padding-top:0px;"
          show-icon>
        </el-alert>
        <kap-editor ref="dataFilterCond" :key="isShow" :placeholder="$t('filterPlaceholder')" height="95" width="99.6%" lang="sql" theme="chrome" v-model="filterCondition"></kap-editor>
      </div>
      <div class="error-msg-box ksd-mt-10" v-if="filterErrorMsg">
        <div class="error-tag">{{$t('errorMsg')}}</div>
        <div v-html="filterErrorMsg"></div>
      </div>
    </template>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button plain size="medium" @click="isShow && handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" v-if="isShow" :disabled="isLoadingNewRange||disabledSave" :loading="isLoadingSave" v-guide.partitionSaveBtn @click="savePartitionConfirm" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../../../store'
import locales from './locales'
import store, { types } from './store'
import { timeDataType, dateFormats } from '../../../../../config'
import NModel from '../../ModelEdit/model.js'
// import { titleMaps, cancelMaps, confirmMaps, getSubmitData } from './handler'
import { objectClone, isSubPartitionType } from '../../../../../util'
import { handleSuccess, transToUTCMs } from 'util/business'
import { handleSuccessAsync, handleError } from 'util/index'
vuex.registerModule(['modals', 'ModelSaveConfig'], store)

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'datasourceActions'
    ]),
    // Store数据注入
    ...mapState('ModelSaveConfig', {
      isShow: state => state.isShow,
      mode: state => state.form.mode,
      modelDesc: state => state.form.modelDesc,
      modelInstance: state => state.form.modelInstance || state.form.modelDesc && new NModel(state.form.modelDesc) || null,
      isChangeModelLayout: state => state.form.isChangeModelLayout,
      callback: state => state.callback
    }),
    ...mapState('DimensionsModal', {
      otherColumns: state => state.otherColumns
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('ModelSaveConfig', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    }),
    // 后台接口请求
    ...mapActions({
      updateProject: 'UPDATE_PROJECT',
      loadHiveInProject: 'LOAD_HIVE_IN_PROJECT',
      saveKafka: 'SAVE_KAFKA',
      loadDataSourceByProject: 'LOAD_DATASOURCE',
      saveSampleData: 'SAVE_SAMPLE_DATA',
      setModelPartition: 'MODEL_PARTITION_SET',
      fetchNewestModelRange: 'GET_MODEL_NEWEST_RANGE',
      fetchPartitionFormat: 'FETCH_PARTITION_FORMAT',
      checkFilterConditon: 'CHECK_FILTER_CONDITION',
      fetchSegments: 'FETCH_SEGMENTS'
    })
  },
  locales
})
export default class ModelPartitionModal extends Vue {
  isLoading = false
  isFormShow = false
  isLoadingNewRange = false
  isLoadingFormat = false
  isLoadingSave = false
  partitionMeta = {
    table: '',
    column: '',
    format: '',
    multiPartition: ''
  }
  timeDataType = timeDataType
  rules = {
    dataRangeVal: [{
      validator: this.validateRange, trigger: 'blur'
    }]
  }
  partitionRules = {
    column: [{validator: this.validateBrokenColumn, trigger: 'change'}]
  }
  filterErrorMsg = ''
  prevPartitionMeta = {
    table: '',
    column: '',
    format: '',
    multiPartition: ''
  }
  buildType = 'incremental'
  isShowWarning = false
  importantChange = false
  isExpand = false

  toggleShowPartition () {
    this.isExpand = !this.isExpand
  }
  get disabledSave () {
    if (this.buildType === 'incremental' && this.partitionMeta.table && this.partitionMeta.column && this.partitionMeta.format || this.buildType === 'fullLoad') {
      return false
    } else {
      return true
    }
  }
  get buildTips () {
    if (this.buildType === 'incremental') {
      return this.$t('incrementalTips')
    } else if (this.buildType === 'fullLoad') {
      return this.$t('fullLoadTips', {storageSize: Vue.filter('dataSize')(this.modelDesc.storage)})
    }
  }
  handChangeBuildType (val) {
    if (typeof this.modelDesc.available_indexes_count === 'number' && this.modelDesc.available_indexes_count > 0) {
      this.isShowWarning = true
    }
    if (val === 'incremental' && !this.partitionMeta.table) {
      this.partitionMeta.table = this.partitionTables[0].alias
    }
  }
  validateRange (rule, value, callback) {
    const [ startValue, endValue ] = value
    if ((startValue && endValue && transToUTCMs(startValue) < transToUTCMs(endValue)) || !startValue && !endValue) {
      callback()
    } else {
      callback(new Error(this.$t('invaildDate')))
    }
  }
  validateBrokenColumn (rule, value, callback) {
    if (value) {
      if (this.checkIsBroken(this.brokenPartitionColumns, value)) {
        return callback(new Error(this.$t('noColumnFund')))
      }
    }
    if (!value && this.partitionMeta.table) {
      return callback(new Error(this.$t('pleaseInputColumn')))
    }
    callback()
  }
  modelBuildMeta = {
    dataRangeVal: [],
    isLoadExisted: false
  }
  checkIsBroken (brokenKeys, key) {
    if (key) {
      return ~brokenKeys.indexOf(key)
    }
    return false
  }
  async handleLoadFormat () {
    try {
      this.isLoadingFormat = true
      const response = await this.fetchPartitionFormat({ project: this.currentSelectedProject, table: this.selectedTable.name, partition_column: this.partitionMeta.column })
      this.partitionMeta.format = await handleSuccessAsync(response)
      this.isLoadingFormat = false
    } catch (e) {
      this.isLoadingFormat = false
      handleError(e)
    }
  }
  filterCondition = ''
  originFilterCondition = ''
  dateFormats = dateFormats
  get partitionTitle () {
    if (this.mode === 'saveModel') {
      return this.$t('modelSaveSet')
    } else {
      return this.$t('modelPartitionSet')
    }
  }
  get partitionTables () {
    let result = []
    if (this.isShow && this.modelInstance) {
      Object.values(this.modelInstance.tables).forEach((nTable) => {
        if (nTable.kind === 'FACT') {
          result.push(nTable)
        }
      })
    }
    return result
  }
  // get showDataRange () {
  //   // 分区列有空值或者和历史值一样
  //   if (!this.partitionMeta.table || !this.partitionMeta.column || this.partitionMeta.table + '.' + this.partitionMeta.column === this.modelInstance.his_partition_desc.partition_date_column) {
  //     return false
  //   }
  //   return true
  // }
  // 获取破损的partition keys
  get brokenPartitionColumns () {
    if (this.partitionMeta.table) {
      let ntable = this.modelInstance.getTableByAlias(this.partitionMeta.table)
      if (ntable) {
        return this.modelInstance.getBrokenModelLinksKeys(ntable.guid, [this.partitionMeta.column])
      } else {
        return []
      }
    }
    return []
  }
  get selectedTable () {
    if (this.partitionMeta.table) {
      for (let i = 0; i < this.partitionTables.length; i++) {
        if (this.partitionTables[i].alias === this.partitionMeta.table) {
          return this.partitionTables[i]
        }
      }
    }
  }
  get columns () {
    if (!this.isShow || this.partitionMeta.table === '') {
      return []
    }
    let result = []
    let factTable = this.modelInstance.getFactTable()
    if (factTable) {
      factTable.columns.forEach((x) => {
        if (isSubPartitionType(x.datatype)) {
          result.push(x)
        }
      })
    }
    // 暂不支持CC列做分区列
    // let ccColumns = this.modelInstance.getComputedColumns()
    // let cloneCCList = objectClone(ccColumns)
    // cloneCCList.forEach((x) => {
    //   let cc = {
    //     name: x.columnName,
    //     datatype: x.datatype
    //   }
    //   result.push(cc)
    // })
    return result
  }
  // get formatList () {
  //   if (!this.partitionMeta.column) {
  //     return []
  //   }
  //   let partitionColumn = this.getColumnInfo(this.partitionMeta.column)
  //   if (!partitionColumn) {
  //     return []
  //   } else {
  //     if (timeDataType.indexOf(partitionColumn.datatype) === -1) {
  //       this.partitionMeta.format = 'yyyy-MM-dd'
  //       return this.integerFormat
  //     } else {
  //       this.partitionMeta.format = ''
  //       return this.dateFormat
  //     }
  //   }
  // }
  getColumnInfo (column) {
    if (this.selectedTable) {
      let len = this.selectedTable.columns && this.selectedTable.columns.length || 0
      for (let i = 0; i < len; i++) {
        const col = this.selectedTable.columns[i]
        if (col.name === column) {
          return col
        }
      }
    }
  }
  @Watch('isShow')
  initModeDesc () {
    if (this.isShow) {
      this.modelBuildMeta.dataRangeVal = []
      // this.$nextTick(() => {
      //   this.$refs.partitionForm && this.$refs.partitionForm.validate()
      // })
      if (this.modelDesc.uuid && !(this.modelDesc.partition_desc && this.modelDesc.partition_desc.partition_date_column)) {
        this.buildType = 'fullLoad'
      }
      if (this.modelDesc && this.modelDesc.partition_desc && this.modelDesc.partition_desc.partition_date_column) {
        let named = this.modelDesc.partition_desc.partition_date_column.split('.')
        this.partitionMeta.table = this.prevPartitionMeta.table = named[0]
        this.partitionMeta.column = this.prevPartitionMeta.column = named[1]
        this.partitionMeta.format = this.prevPartitionMeta.format = this.modelDesc.partition_desc.partition_date_format
        this.partitionMeta.multiPartition = this.prevPartitionMeta.multiPartition = this.modelDesc.multi_partition_desc && this.modelDesc.multi_partition_desc.columns[0] && this.modelDesc.multi_partition_desc.columns[0].split('.')[1] || ''
      } else {
        this.partitionMeta.table = this.partitionTables[0].alias // 默认增量构建选择事实表
      }
      this.filterCondition = this.modelDesc.filter_condition
      this.originFilterCondition = this.modelDesc.filter_condition
    } else {
      this.resetForm()
    }
  }
  @Watch('filterCondition')
  filterConditionChange (val, oldVal) {
    if (val !== oldVal) {
      this.resetMsg()
    }
  }
  resetMsg () {
    this.filterErrorMsg = ''
  }
  partitionTableChange () {
    this.partitionMeta.column = ''
    this.partitionMeta.format = ''
    this.partitionMeta.multiPartition = ''
    // this.$refs.partitionForm.validate()
  }
  // partitionColumnChange () {
  //   this.partitionMeta.format = 'yyyy-MM-dd'
  //   this.$refs.partitionForm.validate()
  // }
  resetForm () {
    this.partitionMeta = {
      table: '',
      column: '',
      format: '',
      multiPartition: ''
    }
    this.prevPartitionMeta = { table: '', column: '', format: '', multiPartition: '' }
    this.filterCondition = ''
    this.isLoadingSave = false
    this.isLoadingFormat = false
    this.isShowWarning = false
  }

  get isChangeToFullLoad () {
    return this.prevPartitionMeta.table && this.buildType === 'fullLoad'
  }

  get isChangePartition () {
    return (this.prevPartitionMeta.table !== this.partitionMeta.table || this.prevPartitionMeta.column !== this.partitionMeta.column || this.prevPartitionMeta.format !== this.partitionMeta.format || this.prevPartitionMeta.multiPartition !== this.partitionMeta.multiPartition) && this.buildType === 'incremental'
  }

  async savePartitionConfirm () {
    await (this.$refs.rangeForm && this.$refs.rangeForm.validate()) || Promise.resolve()
    await (this.$refs.partitionForm && this.$refs.partitionForm.validate()) || Promise.resolve()
    let isOnlySave = true
    if (typeof this.modelDesc.available_indexes_count === 'number' && this.modelDesc.available_indexes_count > 0) {
      // if (this.prevPartitionMeta.table && this.buildType === 'fullLoad') {
      //   await kapConfirm(this.$t('changeSegmentTip2', {modelName: this.modelDesc.name}), '', this.$t('kylinLang.common.tip'))
      // } else if ((this.prevPartitionMeta.table !== this.partitionMeta.table || this.prevPartitionMeta.column !== this.partitionMeta.column || this.prevPartitionMeta.format !== this.partitionMeta.format) && this.buildType === 'incremental') {
      //   await kapConfirm(this.$t('changeSegmentTip1', {tableColumn: `${this.partitionMeta.table}.${this.partitionMeta.column}`, dateType: this.partitionMeta.format, modelName: this.modelDesc.name}), '', this.$t('kylinLang.common.tip'))
      // }
      if (this.isChangeToFullLoad || this.isChangePartition) {
        this.importantChange = true
        // await kapConfirm(this.$t('changeSegmentTips'), {confirmButtonText: this.$t('kylinLang.common.save'), type: 'warning', dangerouslyUseHTMLString: true}, this.$t('kylinLang.common.tip'))
        const res = await this.callGlobalDetailDialog({
          msg: this.$t('changeSegmentTips'),
          title: this.$t('kylinLang.common.tip'),
          dialogType: 'warning',
          showDetailBtn: false,
          isSubSubmit: true,
          dangerouslyUseHTMLString: true,
          submitSubText: this.$t('kylinLang.common.save'),
          submitText: this.$t('saveAndLoad')
        })
        isOnlySave = res.isOnlySave
      } else if (this.isChangeModelLayout || this.originFilterCondition !== this.filterCondition) {
        this.importantChange = true
        const res = await this.callGlobalDetailDialog({
          msg: this.$t('purgeSegmentDataTips', {storageSize: Vue.filter('dataSize')(this.modelInstance.storage)}),
          title: this.$t('kylinLang.common.tip'),
          dialogType: 'warning',
          showDetailBtn: false,
          isSubSubmit: true,
          submitSubText: this.$t('kylinLang.common.save'),
          submitText: this.$t('saveAndLoad')
        })
        isOnlySave = res.isOnlySave
      } else {
        this.importantChange = false
      }
      this.savePartition(isOnlySave)
    } else {
      this.savePartition(isOnlySave)
    }
  }

  savePartition (isOnlySave) {
    this.modelDesc.partition_desc = this.modelDesc.partition_desc || {}
    let hasSetDate = this.partitionMeta.table && this.partitionMeta.column && this.buildType === 'incremental'
    if (this.modelDesc && this.partitionMeta.table && this.partitionMeta.column && this.buildType === 'incremental') {
      this.modelDesc.partition_desc.partition_date_column = hasSetDate ? this.partitionMeta.table + '.' + this.partitionMeta.column : ''
    } else {
      this.modelDesc.partition_desc.partition_date_column = ''
    }
    if (this.partitionMeta.multiPartition) {
      this.modelDesc.multi_partition_desc = {
        ...this.modelInstance.multi_partition_desc || {},
        columns: [this.partitionMeta.table + '.' + this.partitionMeta.multiPartition]
      }
    } else {
      this.modelDesc.multi_partition_desc = null
    }
    this.modelDesc.partition_desc.partition_date_format = this.partitionMeta.format
    this.modelDesc.filter_condition = this.filterCondition
    this.modelDesc.project = this.currentSelectedProject
    if (this.modelBuildMeta.dataRangeVal[0] && this.modelBuildMeta.dataRangeVal[1]) {
      this.modelDesc.start = (+transToUTCMs(this.modelBuildMeta.dataRangeVal[0]))
      this.modelDesc.end = (+transToUTCMs(this.modelBuildMeta.dataRangeVal[1]))
    }
    this.modelDesc.other_columns = this.otherColumns.length ? this.otherColumns : this.getOtherColumns()
    if (this.mode === 'saveModel') {
      this.isLoadingSave = true
      const checkData = objectClone(this.modelDesc)
      // 如果未选择partition 把partition desc 设置为null
      if (!(checkData && checkData.partition_desc && checkData.partition_desc.partition_date_column) || this.buildType === 'fullLoad') {
        checkData.partition_desc = null
      }
      this.checkFilterConditon(checkData).then((res) => {
        handleSuccess(res, async (data) => {
          // TODO HA 模式时 post 等接口需要等待同步完去刷新列表
          // await handleWaiting()
          if (!this.importantChange && 'rebuild_index' in data && data.rebuild_index) {
            try {
              const res = await this.callGlobalDetailDialog({
                msg: this.$t('editCCBuildTip'),
                title: this.$t('kylinLang.common.tip'),
                dialogType: 'warning',
                showDetailBtn: false,
                isSubSubmit: true,
                submitSubText: this.$t('kylinLang.common.save'),
                submitText: this.$t('saveAndLoad'),
                needConcelReject: true
              })
              this.handleClose(true, res.isOnlySave)
              this.isLoadingSave = false
            } catch (e) {
              this.isLoadingSave = false
            }
          } else {
            this.handleClose(true, isOnlySave)
            this.isLoadingSave = false
          }
        })
      }, (errorRes) => {
        this.filterErrorMsg = errorRes.data.msg
        this.isLoadingSave = false
        this.$nextTick(() => {
          this.$el.querySelector('.error-msg-box') && this.$el.querySelector('.error-msg-box').scrollIntoView()
        })
      })
    } else {
      this.handleClose(true, isOnlySave)
    }
  }
  handleClose (isSubmit, isOnlySave) {
    this.isLoadingFormat = false
    this.modelDesc.save_only = isOnlySave
    this.filterErrorMsg = ''
    // 不把这个信息记录下来的话，300 延迟后，modelDesc 就 undefined 了
    let temp = objectClone(this.modelDesc)
    setTimeout(() => {
      this.callback && this.callback({
        isSubmit: isSubmit,
        isPurgeSegment: this.isChangePartition,
        data: temp
      })
      this.hideModal()
      this.resetModalForm()
    }, 300)
  }
  showToolTip (value) {
    let len = 0
    value.split('').forEach((v) => {
      if (/[\u4e00-\u9fa5]/.test(v)) {
        len += 2
      } else {
        len += 1
      }
    })
    return len <= 15
  }
  getOtherColumns () {
    const { simplified_dimensions } = this.modelDesc
    const { tables } = this.modelInstance
    const selectDimensionIds = simplified_dimensions.map(it => it.column)
    let allColumns = []
    const others = []
    Object.values(tables).forEach(it => {
      it.columns && (allColumns = [...allColumns, ...it.columns.map(item => ({column: `${it.alias}.${item.name}`, name: item.name, datatype: item.datatype}))])
    })
    allColumns.filter(item => !selectDimensionIds.includes(item.column)).forEach((it, index, self) => {
      const names = self.map(it => it.name)
      const [table, column] = it.column.split('.')
      if (names.indexOf(it.name) !== names.lastIndexOf(it.name)) {
        others.push({...it, name: `${column}_${table}`})
      } else {
        others.push(it)
      }
    })
    return others
  }
}
</script>

<style lang="less" scoped>
@import '../../../../../assets/styles/variables.less';
.model-partition-dialog {
  .error-msg-box {
    border: 1px solid @line-border-color;
    max-height: 55px;
    overflow: auto;
    font-size: 12px;
    padding: 10px;
    .error-tag {
      color: @error-color-1;
    }
  }
  .divide-block {
    color: @base-color;
    position: relative;
    text-align: center;
    margin-top: 5px;
    span {
      cursor: pointer;
    }
    .arrow {
      transform: rotate(90deg);
      margin-left: 3px;
      font-size: 5px;
      color: @base-color;
      position: absolute;
      top: 15px;
    }
    .divider {
      margin: 10px 0;
      border-bottom: 1px solid @line-split-color;
    }
  }
  .item-desc {
    font-size: 12px;
    line-height: 1;
  }
  .where-area {
    margin-top:20px;
  }
  // .error-msg {display:none}
  // .is-broken {
  //   .el-input__inner{
  //     border:solid 1px @color-danger;
  //   }
  //   .error-msg {
  //     color:@color-danger;
  //     display:block;
  //   }
  // }
  .up-performance{
    i {
      color:@normal-color-1;
      margin-right: 7px;
    }
    span {
      color:@normal-color-1;
      margin-left: 7px;
    }
  }
  .down-performance{
    i {
      color:@error-color-1;
      margin-right: 7px;
    }
    span {
      color:@error-color-1;
      margin-left: 7px;
    }
  }
}
.table-column-name {
  display: inline-block;
  width: 143px;
  overflow: hidden;
  text-overflow: ellipsis;
}

</style>
