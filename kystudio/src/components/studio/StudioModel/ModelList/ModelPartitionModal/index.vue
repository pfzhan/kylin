<template>
  <el-dialog 
    :title="$t('modelPartitionSet')"
    width="560px"
    append-to-body
    :visible="isShow" 
    class="model-partition-dialog" 
    @close="isShow && handleClose(false)" 
    :close-on-press-escape="false" 
    :close-on-click-modal="false">     
    <!-- <div class="ky-list-title" v-if="!(modelInstance && modelInstance.uuid) && partitionMeta.table && partitionMeta.column">{{$t('partitionSet')}}</div> -->
    <el-form :model="modelBuildMeta" ref="partitionForm" :rules="rules"  label-width="85px" label-position="top"> 
      <el-form-item  :label="$t('partitionDateColumn')">
        <el-select :disabled="isLoadingNewRange" v-guide.partitionTable v-model="partitionMeta.table" @change="partitionTableChange" :placeholder="$t('kylinLang.common.pleaseSelect')" style="width:248px" class="ksd-mr-5">
          <el-option :label="$t('noPartition')" value=""></el-option>
          <el-option :label="t.alias" :value="t.alias" v-for="t in partitionTables" :key="t.alias">{{t.alias}}</el-option>
        </el-select><el-select :disabled="isLoadingNewRange"
         v-guide.partitionColumn  v-model="partitionMeta.column" :placeholder="$t('kylinLang.common.pleaseSelect')" filterable style="width:248px">
          <el-option :label="t.name" :value="t.name" v-for="t in columns" :key="t.name">
            <span style="float: left">{{ t.name }}</span>
            <span class="ky-option-sub-info">{{ t.datatype }}</span>
          </el-option>
        </el-select>
      </el-form-item>
    <template v-if="!(modelInstance && modelInstance.uuid) && partitionMeta.table && partitionMeta.column">
      <div class="ky-list-title ksd-mt-14">{{$t('loadRange')}}</div>
      <el-form-item class="custom-load ksd-mt-10"  prop="dataRangeVal" :rule="[{required: true, trigger: 'blur', message: this.$t('dataRangeValValid')}, {
      validator: this.validateRange, trigger: 'blur'
    }]">
        <div class="ky-no-br-space">
          <el-date-picker
              type="datetime"
              v-model="modelBuildMeta.dataRangeVal[0]"
              :is-auto-complete="true"
              class="ksd-mr-5"
              :disabled="modelBuildMeta.isLoadExisted || isLoadingNewRange"
              :picker-options="{ disabledDate: time => time.getTime() > modelBuildMeta.dataRangeVal[1] && modelBuildMeta.dataRangeVal[1] !== null }"
              :placeholder="$t('kylinLang.common.startTime')">
            </el-date-picker>
            <el-date-picker
              type="datetime"
              v-model="modelBuildMeta.dataRangeVal[1]"
              :is-auto-complete="true"
              :disabled="modelBuildMeta.isLoadExisted || isLoadingNewRange"
              :picker-options="{ disabledDate: time => time.getTime() < modelBuildMeta.dataRangeVal[0] && modelBuildMeta.dataRangeVal[0] !== null }"
              :placeholder="$t('kylinLang.common.endTime')">
            </el-date-picker>
            <el-tooltip effect="dark" :content="$t('detectAvailableRange')" placement="top">
              <el-button
                v-if="isShow"
                size="medium"
                class="ksd-ml-10"
                :disabled="modelBuildMeta.isLoadExisted"
                :loading="isLoadingNewRange"
                icon="el-icon-ksd-data_range_search"
                @click="handleLoadNewestRange">
              </el-button>
            </el-tooltip>
        </div>
      </el-form-item>
      </template>
    </el-form>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button plain  size="medium" :disabled="isLoadingNewRange" @click="isShow && handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" v-if="isShow" :disabled="isLoadingNewRange" v-guide.partitionSaveBtn plain @click="savePartition" size="medium">{{$t('kylinLang.common.ok')}}</el-button>
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
import { timeDataType } from '../../../../../config'
import NModel from '../../ModelEdit/model.js'
// import { titleMaps, cancelMaps, confirmMaps, getSubmitData } from './handler'
import { isDatePartitionType, objectClone } from '../../../../../util'
import { transToUTCMs, handleError, getGmtDateFromUtcLike } from 'util/business'
import { handleSuccessAsync } from 'util/index'
vuex.registerModule(['modals', 'ModelPartitionModal'], store)

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('ModelPartitionModal', {
      isShow: state => state.isShow,
      modelDesc: state => state.form.modelDesc,
      modelInstance: state => state.form.modelDesc && new NModel(state.form.modelDesc) || null,
      callback: state => state.callback
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('ModelPartitionModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
      updateProject: 'UPDATE_PROJECT',
      loadHiveInProject: 'LOAD_HIVE_IN_PROJECT',
      saveKafka: 'SAVE_KAFKA',
      loadDataSourceByProject: 'LOAD_DATASOURCE',
      saveSampleData: 'SAVE_SAMPLE_DATA',
      setModelPartition: 'MODEL_PARTITION_SET',
      fetchNewestModelRange: 'GET_MODEL_NEWEST_RANGE'
    })
  },
  locales
})
export default class ModelPartitionModal extends Vue {
  isLoading = false
  isFormShow = false
  isLoadingNewRange = false
  factTableColumns = [{tableName: 'DEFAULT.KYLIN_SALES', column: 'PRICE'}]
  lookupTableColumns = [{tableName: 'DEFAULT.KYLIN_CAL_DT', column: 'CAL_DT'}]
  partitionMeta = {
    table: '',
    column: '',
    format: ''
  }
  rules = {
    dataRangeVal: [{
      validator: this.validateRange, trigger: 'blur'
    }]
  }
  validateRange (rule, value, callback) {
    const [ startValue, endValue ] = value
    const isLoadExisted = this.modelBuildMeta.isLoadExisted
    if ((!startValue || !endValue || transToUTCMs(startValue) >= transToUTCMs(endValue)) && !isLoadExisted) {
      callback(new Error(this.$t('invaildDate')))
    } else {
      callback()
    }
  }
  modelBuildMeta = {
    dataRangeVal: [],
    isLoadExisted: false
  }
  async handleLoadNewestRange () {
    this.isLoadingNewRange = true
    try {
      let tableInfo = this.modelInstance.getTableByAlias(this.partitionMeta.table)
      const submitData = {
        project: this.currentSelectedProject,
        table: tableInfo.name,
        partitionColumn: this.partitionMeta.column
      }
      const response = await this.fetchNewestModelRange(submitData)
      const result = await handleSuccessAsync(response)
      const startTime = +result.start_time
      const endTime = +result.end_time
      this.modelBuildMeta.dataRangeVal = [ getGmtDateFromUtcLike(startTime), getGmtDateFromUtcLike(endTime) ]
    } catch (e) {
      handleError(e)
    }
    this.isLoadingNewRange = false
  }
  @Watch('isShow')
  initModelBuldRange () {
    if (this.isShow) {
      this.modelBuildMeta.dataRangeVal = []
    }
  }
  filterCondition = ''
  dateFormat = [
    {label: 'yyyy-MM-dd', value: 'yyyy-MM-dd'},
    {label: 'yyyyMMdd', value: 'yyyyMMdd'},
    {label: 'yyyy-MM-dd HH:mm:ss', value: 'yyyy-MM-dd HH:mm:ss'},
    {label: 'yyyy-MM-dd HH:mm:ss.SSS', value: 'yyyy-MM-dd HH:mm:ss.SSS'}
  ]
  integerFormat = [
    {label: 'yyyy-MM-dd', value: 'yyyy-MM-dd'},
    {label: 'yyyyMMdd', value: 'yyyyMMdd'},
    {label: 'yyyy-MM-dd HH:mm:ss', value: 'yyyy-MM-dd HH:mm:ss'},
    {label: 'yyyy-MM-dd HH:mm:ss.SSS', value: 'yyyy-MM-dd HH:mm:ss.SSS'},
    {label: '', value: ''}
  ]
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
  get formatList () {
    if (!this.partitionMeta.column) {
      return []
    }
    let partitionColumn = this.getColumnInfo(this.partitionMeta.column)
    if (!partitionColumn) {
      return []
    } else {
      if (timeDataType.indexOf(partitionColumn.datatype) === -1) {
        return this.integerFormat
      } else {
        return this.dateFormat
      }
    }
  }
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
    if (this.isShow && this.modelDesc && this.modelDesc.partition_desc && this.modelDesc.partition_desc.partition_date_column) {
      let named = this.modelDesc.partition_desc.partition_date_column.split('.')
      this.partitionMeta.table = named[0]
      this.partitionMeta.column = named[1]
      this.partitionMeta.format = this.modelDesc.partition_desc.partition_date_format
      this.partitionMeta.filter_condition = this.modelDesc.filter_condition
    } else {
      this.resetForm()
    }
  }
  partitionTableChange () {
    this.partitionMeta.column = ''
    this.partitionMeta.format = ''
  }
  resetForm () {
    this.partitionMeta = {
      table: '',
      column: '',
      format: ''
    }
    this.filterCondition = ''
  }
  savePartition () {
    this.$refs.partitionForm.validate((valid) => {
      if (!valid) { return }
      this.modelDesc.partition_desc = this.modelDesc.partition_desc || {}
      let hasSetDate = this.partitionMeta.table && this.partitionMeta.column
      if (this.modelDesc && this.partitionMeta.table && this.partitionMeta.column) {
        this.modelDesc.partition_desc.partition_date_column = hasSetDate ? this.partitionMeta.table + '.' + this.partitionMeta.column : ''
      } else {
        this.modelDesc.partition_desc.partition_date_column = ''
      }
      this.modelDesc.partition_desc.partition_date_format = this.partitionMeta.format
      this.modelDesc.filter_condition = this.filterCondition
      this.modelDesc.project = this.currentSelectedProject
      if (!this.modelInstance.uuid) {
        if (this.modelBuildMeta.isLoadExisted) {
          this.modelDesc.start = null
          this.modelDesc.end = null
        } else {
          this.modelDesc.start = (+transToUTCMs(this.modelBuildMeta.dataRangeVal[0]))
          this.modelDesc.end = (+transToUTCMs(this.modelBuildMeta.dataRangeVal[1]))
        }
      }
      this.handleClose(true)
    })
  }
  handleClose (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback({
        isSubmit: isSubmit,
        data: this.modelDesc
      })
    }, 300)
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
.model-partition-dialog {
  .item-desc {
    font-size: 12px;
    line-height: 1;
  }
  .where-area {
    margin-top:20px;
  }
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

</style>
