<template>
  <!-- 模型构建 -->
    <el-dialog class="model-build" :title="$t('buildIndex')" width="560px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <div>
        <el-alert
          :title="$t('changeBuildTypeTips')"
          type="warning"
          :closable="false"
          class="ksd-mb-10"
          v-if="isShowWarning"
          show-icon>
        </el-alert>
        <div class="ksd-title-label-small ksd-mb-10">{{$t('chooseBuildType')}}</div>
        <div>
          <el-radio-group v-model="buildOrComplete" class="ksd-mb-10">
            <el-radio label="build">{{$t('build')}}</el-radio>
            <common-tip :content="$t('unableComplete')" v-if="!modelDesc.empty_indexes_count">
              <el-radio :disabled="!modelDesc.empty_indexes_count" class="ksd-ml-10" label="complete">{{$t('complete')}}</el-radio>
            </common-tip>
            <el-radio v-else label="complete">{{$t('complete')}}</el-radio>
          </el-radio-group>
        </div>
        <el-select v-model="buildType" class="ksd-mb-10" @change="handChangeBuildType" v-if="buildOrComplete == 'build'" :disabled="!datasourceActions.includes('changeBuildType')">
          <el-option :label="$t('incremental')" value="incremental"></el-option>
          <el-option :label="$t('fullLoad')" value="fullLoad"></el-option>
        </el-select>
      </div>
      <el-alert
        class="ksd-pt-0"
        :title="buildTips"
        type="info"
        :show-background="false"
        :closable="false"
        show-icon>
      </el-alert>
      <div v-if="buildType === 'incremental' && buildOrComplete === 'build'">
        <el-form class="ksd-mb-20" v-if="isExpand" :model="partitionMeta" ref="partitionForm" :rules="partitionRules"  label-width="85px" label-position="top">
          <el-form-item  :label="$t('partitionDateColumn')" class="clearfix">
            <el-row :gutter="5">
              <el-col :span="12">
                <el-select :disabled="isLoadingNewRange || !datasourceActions.includes('changePartition')" v-guide.partitionTable v-model="partitionMeta.table" @change="partitionTableChange" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" style="width:100%">
                  <el-option :label="$t('noPartition')" value=""></el-option>
                  <el-option :label="t.alias" :value="t.alias" v-for="t in partitionTables" :key="t.alias">{{t.alias}}</el-option>
                </el-select>
              </el-col>
              <el-col :span="12" v-if="partitionMeta.table">
                <el-form-item prop="column">
                  <el-select :disabled="isLoadingNewRange || !datasourceActions.includes('changePartition')"
                  v-guide.partitionColumn @change="partitionColumnChange" v-model="partitionMeta.column" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" filterable style="width:100%">
                  <i slot="prefix" class="el-input__icon el-icon-search" v-if="!partitionMeta.column.length"></i>
                    <el-option :label="t.name" :value="t.name" v-for="t in columns" :key="t.name">
                      <el-tooltip :content="t.name" effect="dark" placement="top" :disabled="showToolTip(t.name)"><span style="float: left">{{ t.name | omit(15, '...') }}</span></el-tooltip>
                      <span class="ky-option-sub-info">{{ t.datatype.toLocaleLowerCase() }}</span>
                    </el-option>
                  </el-select>
                </el-form-item>
              </el-col>
            </el-row>
          </el-form-item>
          <el-form-item  :label="$t('dateFormat')" v-if="partitionMeta.table">
            <el-row :gutter="5">
              <el-col :span="12">
                <el-select :disabled="isLoadingFormat || !datasourceActions.includes('changePartition')" v-guide.partitionColumnFormat style="width:100%" @change="partitionColumnFormatChange" v-model="partitionMeta.format" :placeholder="$t('pleaseInputColumn')">
                  <el-option :label="f.label" :value="f.value" v-for="f in dateFormats" :key="f.label"></el-option>
                  <!-- <el-option label="" value="" v-if="partitionMeta.column && timeDataType.indexOf(getColumnInfo(partitionMeta.column).datatype)===-1"></el-option> -->
                </el-select>
              </el-col>
              <el-col :span="12">
                <el-tooltip effect="dark" :content="$t('detectFormat')" placement="top">
                  <div style="display: inline-block;">
                    <el-button
                      size="medium"
                      :loading="isLoadingFormat"
                      :disabled="!datasourceActions.includes('changePartition')"
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
        </el-form>
        <div class="divide-block">
          <span v-if="isExpand" @click="toggleShowPartition">{{$t('showLess')}}</span>
          <span v-else @click="toggleShowPartition">{{$t('showMore')}}</span>
          <div class="divide-line"></div>
        </div>
        <div class="ksd-title-label-small ksd-mb-10">{{$t('pleaseSetDataRange')}}</div>
        <el-form :model="modelBuildMeta" ref="buildForm" :rules="rules" label-position="top">
          <!-- <div class="ky-list-title ksd-mt-14">{{$t('buildRange')}}</div> -->
          <!-- <el-form-item prop="isLoadExisted" class="ksd-mt-10 ksd-mb-2">
            <el-radio class="font-medium" v-model="modelBuildMeta.isLoadExisted" :label="true">
              {{$t('loadExistingData')}}
            </el-radio>
            <div class="item-desc">{{$t('loadExistingDataDesc')}}</div>
          </el-form-item> -->
          <el-form-item prop="dataRangeVal" :class="{'is-error': isShowErrorSegments}" :rule="modelBuildMeta.isLoadExisted ? [] : [{required: true, trigger: 'blur', message: this.$t('dataRangeValValid')}]">
            <!-- <el-radio class="font-medium" v-model="modelBuildMeta.isLoadExisted" :label="false">
              {{$t('customLoadRange')}}
            </el-radio>
            <br/> -->
            <el-alert
              class="ksd-pt-0"
              :title="$t('kylinLang.dataSource.rangeInfoTip')"
              type="info"
              :show-background="false"
              :closable="false"
              show-icon>
            </el-alert>
            <div class="ky-no-br-space" style="height:32px;">
              <el-date-picker
                type="datetime"
                class="ksd-mr-5"
                v-model="modelBuildMeta.dataRangeVal[0]"
                :is-auto-complete="true"
                :disabled="modelBuildMeta.isLoadExisted || isLoadingNewRange"
                @change="resetError"
                value-format="timestamp"
                :format="partitionMeta.format"
                :placeholder="$t('kylinLang.common.startTime')">
              </el-date-picker>
              <el-date-picker
                type="datetime"
                v-model="modelBuildMeta.dataRangeVal[1]"
                :is-auto-complete="true"
                :disabled="modelBuildMeta.isLoadExisted || isLoadingNewRange"
                value-format="timestamp"
                @change="resetError"
                :format="partitionMeta.format"
                :placeholder="$t('kylinLang.common.endTime')">
              </el-date-picker>
              <common-tip :content="noPartition ? $t('partitionFirst'):$t('detectAvailableRange')" placement="top">
                <el-button
                  size="medium"
                  class="ksd-ml-10"
                  v-if="$store.state.project.projectPushdownConfig"
                  :disabled="modelBuildMeta.isLoadExisted || noPartition"
                  :loading="isLoadingNewRange"
                  v-guide.getPartitionRangeDataBtn
                  icon="el-icon-ksd-data_range_search"
                  @click="handleLoadNewestRange">
                </el-button>
              </common-tip>
              <span v-guide.getPartitionRangeData style="position:absolute;width:1px; height:0" @click="handleLoadNewestRange"></span>
              <span v-guide.checkPartitionDataRangeHasData style="position:absolute;width:1px; height:0" v-if="modelBuildMeta.dataRangeVal[0] && modelBuildMeta.dataRangeVal[1]"></span>
            </div>
          </el-form-item>
        </el-form>
        <div class="error-msg" v-if="isShowRangeDateError">{{loadRangeDateError}}</div>
        <div v-if="isShowErrorSegments" class="error_segments">
          <el-alert type="error" :show-background="false" :closable="false" show-icon>
            <span>{{$t('overlapsTips')}}</span>
            <a href="javascript:;" @click="toggleDetail">{{$t('kylinLang.common.seeDetail')}}
              <i class="el-icon-arrow-down" v-show="!showDetail"></i>
              <i class="el-icon-arrow-up" v-show="showDetail"></i>
            </a>
          </el-alert>
          <table class="ksd-table small-size" v-if="showDetail">
            <tr class="ksd-tr" v-for="(s, index) in errorSegments" :key="index">
              <td>{{s.start | toServerGMTDate}}</td>
              <td>{{s.end | toServerGMTDate}}</td>
            </tr>
          </table>
        </div>
      </div>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button plain @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" :loading="btnLoading" v-if="buildOrComplete=='build'" v-guide.setbuildModelRange @click="setbuildModel" :disabled="incrementalDisabled || disableFullLoad" size="medium">{{$t(buildType)}}</el-button>
        <el-button type="primary" :loading="btnLoading" v-else v-guide.setbuildModelRange @click="completeBuildModel" size="medium">{{$t('complete')}}</el-button>
      </div>
    </el-dialog>
</template>
<script>
  import Vue from 'vue'
  import { Component, Watch } from 'vue-property-decorator'
  import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
  import vuex from 'store'
  import { handleError, kapMessage, transToUTCMs, getGmtDateFromUtcLike } from 'util/business'
  import { handleSuccessAsync, transToServerGmtTime, isDatePartitionType, objectClone, kapConfirm } from 'util/index'
  import locales from './locales'
  import store, { types } from './store'
  import NModel from '../../ModelEdit/model.js'
  import { BuildIndexStatus } from 'config/model'
  import { dateFormats } from 'config'
  import moment from 'moment'

  vuex.registerModule(['modals', 'ModelBuildModal'], store)

  @Component({
    computed: {
      ...mapGetters([
        'currentSelectedProject',
        'datasourceActions'
      ]),
      ...mapState('ModelBuildModal', {
        isShow: state => state.isShow,
        type: state => state.type,
        buildOrComp: state => state.buildOrComp,
        isHaveSegment: state => state.isHaveSegment,
        disableFullLoad: state => state.disableFullLoad,
        modelDesc: state => state.form.modelDesc,
        modelInstance: state => state.form.modelInstance || state.form.modelDesc && new NModel(state.form.modelDesc) || null,
        callback: state => state.callback
      })
    },
    methods: {
      ...mapActions({
        buildModel: 'MODEL_BUILD',
        checkDataRange: 'CHECK_DATA_RANGE',
        buildFullLoadModel: 'MODEL_FULLLOAD_BUILD',
        buildIndex: 'BUILD_INDEX',
        fetchNewestModelRange: 'GET_MODEL_NEWEST_RANGE',
        fetchPartitionFormat: 'FETCH_PARTITION_FORMAT',
        updataModel: 'UPDATE_MODEL',
        autoFixSegmentHoles: 'AUTO_FIX_SEGMENT_HOLES',
        setModelPartition: 'MODEL_PARTITION_SET'
      }),
      ...mapMutations('ModelBuildModal', {
        setModal: types.SET_MODAL,
        hideModal: types.HIDE_MODAL,
        setModalForm: types.SET_MODAL_FORM,
        resetModalForm: types.RESET_MODAL_FORM
      }),
      ...mapActions('DetailDialogModal', {
        callGlobalDetailDialog: 'CALL_MODAL'
      })
    },
    locales
  })
  export default class ModelBuildModal extends Vue {
    btnLoading = false
    isLoadingNewRange = false
    modelBuildMeta = {
      dataRangeVal: [],
      isLoadExisted: false
    }
    rules = {
      dataRangeVal: [{
        validator: this.validateRange, trigger: 'blur'
      }]
    }
    loadRangeDateError = ''
    isShowRangeDateError = false
    isShowErrorSegments = false
    showDetail = false
    errorSegments = []
    buildType = ''
    buildOrComplete = 'build'
    partitionMeta = {
      table: '',
      column: '',
      format: 'yyyy-MM-dd'
    }
    prevPartitionMeta = {
      table: '',
      column: '',
      format: ''
    }
    partitionRules = {
      column: [{validator: this.validateBrokenColumn, trigger: 'change'}]
    }
    isLoadingFormat = false
    dateFormats = dateFormats
    isExpand = true
    isShowWarning = false

    toggleDetail () {
      this.showDetail = !this.showDetail
    }
    get noPartition () {
      return !(this.partitionMeta.table && this.partitionMeta.column && this.partitionMeta.format)
    }
    get incrementalDisabled () {
      return !(this.partitionMeta.table && this.partitionMeta.column && this.partitionMeta.format && this.modelBuildMeta.dataRangeVal.length) && this.buildType === 'incremental'
    }
    handChangeBuildType () {
      if (this.isHaveSegment) {
        this.isShowWarning = true
      }
      if (this.buildType === 'incremental' && !this.partitionMeta.table) {
        this.isExpand = true
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

    checkIsBroken (brokenKeys, key) {
      if (key) {
        return ~brokenKeys.indexOf(key)
      }
      return false
    }
    // 获取破损的partition keys
    get brokenPartitionColumns () {
      if (this.partitionMeta.table) {
        let ntable = this.modelInstance.getTableByAlias(this.partitionMeta.table)
        return this.modelInstance.getBrokenModelLinksKeys(ntable.guid, [this.partitionMeta.column])
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

    get columns () {
      if (!this.isShow || this.partitionMeta.table === '') {
        return []
      }
      let result = []
      let factTable = this.modelInstance.getFactTable()
      if (factTable) {
        factTable.columns.forEach((x) => {
          if (isDatePartitionType(x.datatype)) {
            result.push(x)
          }
        })
      }
      let ccColumns = this.modelInstance.getComputedColumns()
      let cloneCCList = objectClone(ccColumns)
      cloneCCList.forEach((x) => {
        let cc = {
          name: x.columnName,
          datatype: x.datatype
        }
        result.push(cc)
      })
      return result
    }

    partitionTableChange () {
      this.partitionMeta.column = ''
      this.partitionMeta.format = ''
      this.$refs.partitionForm.validate()
      this.modelBuildMeta.dataRangeVal = []
    }

    partitionColumnChange () {
      this.partitionMeta.format = 'yyyy-MM-dd'
      this.$refs.partitionForm.validate()
      this.modelBuildMeta.dataRangeVal = []
    }

    partitionColumnFormatChange () {
      this.modelBuildMeta.dataRangeVal = []
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

    toggleShowPartition () {
      this.isExpand = !this.isExpand
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

    get buildTips () {
      if (this.buildOrComplete === 'build') {
        if (this.buildType === 'incremental') {
          return this.$t('incrementalTips')
        } else if (this.buildType === 'fullLoad') {
          return this.$t('fullLoadTips', {storageSize: Vue.filter('dataSize')(this.modelDesc.storage)})
        }
      } else {
        return this.$t('completeTips')
      }
    }
    get format () {
      return this.modelDesc.partition_desc && this.modelDesc.partition_desc.partition_date_format || 'yyyy-MM-dd'
    }
    validateRange (rule, value, callback) {
      const [ startValue, endValue ] = value
      let format = ''
      switch (this.partitionMeta.format) {
        case 'yyyy-MM-dd':
        case 'yyyyMMdd':
        case 'yyyy/MM/dd':
          format = 'YYYY/MM/DD'
          break
        case 'yyyy-MM':
        case 'yyyyMM':
          format = 'YYYY/MM'
          break
      }
      const formatTimestampStart = !format ? startValue : (startValue && new Date(moment(new Date(startValue)).format(format)).getTime())
      const formatTimestampEnd = !format ? endValue : (endValue && new Date(moment(new Date(endValue)).format(format)).getTime())
      const isLoadExisted = this.modelBuildMeta.isLoadExisted

      if ((!startValue || !endValue || transToUTCMs(formatTimestampStart) > transToUTCMs(formatTimestampEnd)) && !isLoadExisted) {
        callback(new Error(this.$t('invaildDate')))
      } else if (startValue && endValue && transToUTCMs(formatTimestampStart) === transToUTCMs(formatTimestampEnd) && !isLoadExisted) {
        callback(new Error(this.$t('invaildDateNoEqual')))
      } else {
        callback()
      }
    }
    @Watch('isShow')
    initModelBuldRange () {
      if (this.isShow) {
        this.buildType = this.type
        this.buildOrComplete = this.buildOrComp
        this.modelBuildMeta.dataRangeVal = []
        if (this.modelDesc.last_build_end && this.buildType === 'incremental') {
          let lastBuildDate = getGmtDateFromUtcLike(+this.modelDesc.last_build_end)
          if (lastBuildDate) {
            this.modelBuildMeta.dataRangeVal.push(lastBuildDate, lastBuildDate)
          }
        }
        this.$nextTick(() => {
          this.$refs.partitionForm && this.$refs.partitionForm.validate()
        })
        if (this.modelDesc && this.modelDesc.partition_desc && this.modelDesc.partition_desc.partition_date_column && this.buildType === 'incremental') {
          let named = this.modelDesc.partition_desc.partition_date_column.split('.')
          this.partitionMeta.table = this.prevPartitionMeta.table = named[0]
          this.partitionMeta.column = this.prevPartitionMeta.column = named[1]
          this.partitionMeta.format = this.prevPartitionMeta.format = this.modelDesc.partition_desc.partition_date_format
          this.isExpand = false
        } else {
          this.isExpand = true
        }
        this.isShowWarning = false
      } else {
        this.modelBuildMeta.dataRangeVal = []
        this.resetForm()
      }
    }
    resetForm () {
      this.partitionMeta = {
        table: '',
        column: '',
        format: ''
      }
      this.prevPartitionMeta = { table: '', column: '', format: '' }
      this.filterCondition = ''
      this.isLoadingSave = false
      this.isLoadingFormat = false
    }
    async handleLoadNewestRange () {
      this.isLoadingNewRange = true
      this.resetError()
      const partition_desc = {
        partition_date_column: this.partitionMeta.table + '.' + this.partitionMeta.column,
        partition_date_format: this.partitionMeta.format
      }
      try {
        const submitData = {
          project: this.currentSelectedProject,
          model: this.modelDesc.uuid,
          partition_desc: partition_desc
        }
        const response = await this.fetchNewestModelRange(submitData)
        if (submitData.model !== this.modelDesc.uuid) { // 避免ajax耗时太长导致会覆盖新的model的load range数据
          return
        }
        if (response.body.code === '000') {
          const result = await handleSuccessAsync(response)
          const startTime = +result.start_time
          const endTime = +result.end_time
          this.modelBuildMeta.dataRangeVal = [ getGmtDateFromUtcLike(startTime), getGmtDateFromUtcLike(endTime) ]
        } else if (response.body.code === '999') {
          this.loadRangeDateError = response.body.msg
          this.isShowRangeDateError = true
        }
      } catch (e) {
        handleError(e)
      }
      this.isLoadingNewRange = false
    }
    resetError () {
      this.loadRangeDateError = ''
      this.isShowRangeDateError = false
      this.isShowErrorSegments = false
      this.errorSegments = []
      this.showDetail = false
    }
    closeModal (isSubmit) {
      this.isLoadingNewRange = false
      this.btnLoading = false
      this.$refs.buildForm && this.$refs.buildForm.resetFields()
      this.isShowErrorSegments = false
      this.errorSegments = []
      this.showDetail = false
      this.resetError()
      this.hideModal()
      setTimeout(() => {
        this.callback && this.callback(isSubmit)
        this.resetModalForm()
      }, 200)
    }
    _buildModel ({start, end, modelId, partition_desc, segment_holes}) {
      this.buildModel({
        model_id: modelId,
        data: {
          start: start,
          end: end,
          partition_desc: partition_desc,
          segment_holes: segment_holes || [],
          project: this.currentSelectedProject
        }
      }).then(() => {
        this.btnLoading = false
        kapMessage(this.$t('kylinLang.common.submitSuccess'))
        this.closeModal(true)
        this.$emit('refreshModelList')
      }, (res) => {
        this.btnLoading = false
        res && handleError(res)
      })
    }
    async setbuildModel () {
      this.btnLoading = true
      try {
        if (this.buildType === 'incremental' && this.buildOrComplete === 'build') {
          this.$refs.buildForm.validate(async (valid) => {
            if (!valid) {
              this.btnLoading = false
              return
            }
            await (this.$refs.rangeForm && this.$refs.rangeForm.validate()) || Promise.resolve()
            await (this.$refs.partitionForm && this.$refs.partitionForm.validate()) || Promise.resolve()
            const partition_desc = {}
            if (typeof this.modelDesc.available_indexes_count === 'number' && this.modelDesc.available_indexes_count > 0) {
              if (this.prevPartitionMeta.table !== this.partitionMeta.table || this.prevPartitionMeta.column !== this.partitionMeta.column || this.prevPartitionMeta.format !== this.partitionMeta.format) {
                await kapConfirm(this.$t('changeSegmentTip1', {tableColumn: `${this.partitionMeta.table}.${this.partitionMeta.column}`, dateType: this.partitionMeta.format, modelName: this.modelDesc.name}), '', this.$t('kylinLang.common.tip'))
              }
            }
            partition_desc.partition_date_column = this.partitionMeta.table + '.' + this.partitionMeta.column
            partition_desc.partition_date_format = this.partitionMeta.format
            let start = null
            let end = null
            if (!this.modelBuildMeta.isLoadExisted) {
              start = transToUTCMs(this.modelBuildMeta.dataRangeVal[0])
              end = transToUTCMs(this.modelBuildMeta.dataRangeVal[1])
            }
            // 如果切换分区列或者构建方式，会清空segment，不用检测
            const isChangePatition = this.prevPartitionMeta.table && (this.prevPartitionMeta.table !== this.partitionMeta.table || this.prevPartitionMeta.column !== this.partitionMeta.column || this.prevPartitionMeta.format !== this.partitionMeta.format)
            const isChangeBuildType = !this.prevPartitionMeta.table && this.isHaveSegment
            if (isChangePatition || isChangeBuildType) {
              this._buildModel({start: start, end: end, modelId: this.modelDesc.uuid, partition_desc: partition_desc})
            } else {
              const res = await this.checkDataRange({modelId: this.modelDesc.uuid, project: this.currentSelectedProject, start: start, end: end})
              const data = await handleSuccessAsync(res)
              if (data.overlap_segments.length) {
                this.btnLoading = false
                this.isShowErrorSegments = true
                this.errorSegments = data.overlap_segments
              } else if (data.segment_holes.length) {
                const tableData = []
                let selectSegmentHoles = []
                const segmentHoles = data.segment_holes
                segmentHoles.forEach((seg) => {
                  const obj = {}
                  obj['start'] = transToServerGmtTime(seg.start)
                  obj['end'] = transToServerGmtTime(seg.end)
                  obj['date_range_start'] = seg.start
                  obj['date_range_end'] = seg.end
                  tableData.push(obj)
                })
                try {
                  await this.callGlobalDetailDialog({
                    msg: this.$t('segmentHoletips', {modelName: this.modelDesc.name}),
                    title: this.$t('fixSegmentTitle'),
                    detailTableData: tableData,
                    detailColumns: [
                      {column: 'start', label: this.$t('kylinLang.common.startTime')},
                      {column: 'end', label: this.$t('kylinLang.common.endTime')}
                    ],
                    isShowSelection: true,
                    dialogType: 'warning',
                    showDetailBtn: false,
                    needResolveCancel: true,
                    cancelText: this.$t('ignore'),
                    submitText: this.$t('fixAndBuild'),
                    onlyCloseDialogReject: true,
                    customCallback: async (segments) => {
                      selectSegmentHoles = segments.map((seg) => {
                        return {start: seg.date_range_start, end: seg.date_range_end}
                      })
                      try {
                        this._buildModel({start: start, end: end, modelId: this.modelDesc.uuid, partition_desc: partition_desc, segment_holes: selectSegmentHoles})
                      } catch (e) {
                        handleError(e)
                      }
                    }
                  })
                  this._buildModel({start: start, end: end, modelId: this.modelDesc.uuid, partition_desc: partition_desc})
                } catch (e) {
                  this.btnLoading = false
                }
              } else {
                this._buildModel({start: start, end: end, modelId: this.modelDesc.uuid, partition_desc: partition_desc})
              }
            }
          })
        } else if (this.buildType === 'fullLoad' && this.buildOrComplete === 'build') {
          if (this.modelDesc && this.modelDesc.partition_desc && this.modelDesc.partition_desc.partition_date_column) {
            await kapConfirm(this.$t('changeBuildTypeTipsConfirm', {modelName: this.modelDesc.name}), '', this.$t('kylinLang.common.tip'))
            this.btnLoading = true
            await this.setModelPartition({modelId: this.modelDesc.uuid, project: this.currentSelectedProject, partition_desc: null})
          }
          await this.buildFullLoadModel({
            model_id: this.modelDesc.uuid,
            start: null,
            end: null,
            project: this.currentSelectedProject
          })
          this.btnLoading = false
          kapMessage(this.$t('kylinLang.common.submitSuccess'))
          this.closeModal(true)
          this.$emit('refreshModelList')
        }
      } catch (e) {
        this.btnLoading = false
        handleError(e)
      }
    }
    async completeBuildModel () {
      if (this.modelDesc.segment_holes.length) {
        const segmentHoles = this.modelDesc.segment_holes
        try {
          const tableData = []
          let selectSegmentHoles = []
          segmentHoles.forEach((seg) => {
            const obj = {}
            obj['start'] = transToServerGmtTime(seg.date_range_start)
            obj['end'] = transToServerGmtTime(seg.date_range_end)
            obj['date_range_start'] = seg.date_range_start
            obj['date_range_end'] = seg.date_range_end
            tableData.push(obj)
          })
          await this.callGlobalDetailDialog({
            msg: this.$t('segmentHoletips', {modelName: this.modelDesc.name}),
            title: this.$t('fixSegmentTitle'),
            detailTableData: tableData,
            detailColumns: [
              {column: 'start', label: this.$t('kylinLang.common.startTime')},
              {column: 'end', label: this.$t('kylinLang.common.endTime')}
            ],
            isShowSelection: true,
            dialogType: 'warning',
            showDetailBtn: false,
            needResolveCancel: true,
            cancelText: this.$t('ignore'),
            submitText: this.$t('fixAndBuild'),
            customCallback: async (segments) => {
              selectSegmentHoles = segments.map((seg) => {
                return {start: seg.date_range_start, end: seg.date_range_end}
              })
              try {
                await this.autoFixSegmentHoles({project: this.currentSelectedProject, model_id: this.modelDesc.uuid, segment_holes: selectSegmentHoles})
              } catch (e) {
                handleError(e)
              }
              this.confirmBuild()
            }
          })
          this.confirmBuild()
        } catch (e) {
          e !== 'cancel' && handleError(e)
        }
      } else {
        this.confirmBuild()
      }
    }

    async confirmBuild () {
      try {
        this.btnLoading = true
        let res = await this.buildIndex({
          project: this.currentSelectedProject,
          model_id: this.modelDesc.uuid
        })
        let data = await handleSuccessAsync(res)
        this.handleBuildIndexTip(data)
        this.closeModal(true)
        this.$emit('refreshModelList')
      } catch (e) {
        handleError(e)
      } finally {
        this.btnLoading = false
      }
    }
    handleBuildIndexTip (data) {
      let tipMsg = ''
      if (data.type === BuildIndexStatus.NORM_BUILD) {
        tipMsg = this.$t('kylinLang.model.buildIndexSuccess')
        this.$message({message: tipMsg, type: 'success'})
        return
      }
      if (data.type === BuildIndexStatus.NO_LAYOUT) {
        tipMsg = this.$t('kylinLang.model.buildIndexFail2', {indexType: this.$t('kylinLang.model.index')})
      } else if (data.type === BuildIndexStatus.NO_SEGMENT) {
        tipMsg += this.$t('kylinLang.model.buildIndexFail1', {modelName: this.modelDesc.name})
      }
      this.$confirm(tipMsg, this.$t('kylinLang.common.notice'), {showCancelButton: false, type: 'warning', dangerouslyUseHTMLString: true})
    }
    created () {
      this.$on('buildModel', this._buildModel)
    }
  }
</script>
<style lang="less">
@import '../../../../../assets/styles/variables.less';
  .model-build {
    .item-desc {
      font-size: 12px;
      line-height: 1;
    }
    .error-msg {
      color: @error-color-1;
      font-size: 12px;
      margin-top: 5px;
    }
    .error_segments a:hover {
      text-decoration: none;
    }
    .divide-block {
      color: @base-color;
      font-size: 12px;
      text-align: center;
      cursor: pointer;
    }
    .divide-line {
      border-top: 1px solid @line-border-color;
      margin-top: 10px;
      margin-bottom: 20px;
    }
  }
</style>
