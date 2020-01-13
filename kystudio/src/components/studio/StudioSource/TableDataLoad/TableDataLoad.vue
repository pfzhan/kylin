<template>
  <div class="table-data-load">
    <div class="info-group">
      <div class="info-row">
        <span class="info-label font-medium">{{$t('tableName')}}</span>
        <span class="info-value">{{table.name}}</span>
      </div>
      <div class="info-row">
        <span class="info-label font-medium">{{$t('partitionKey')}}</span>
        <span class="info-value">
          <span v-if="table.partitionColumn">{{table.partitionColumn}}</span>
          <span v-else>{{$t('noPartition')}}</span>
        </span><el-tooltip effect="dark" :content="$t('partitionSetting')" placement="top">
          <span class="edit-btn ksd-ml-5">
            <i class="el-icon-ksd-table_edit" @click="editPartition"></i>
          </span>
        </el-tooltip>
      </div>
      <div class="info-row" v-show="table.partitionColumn">
        <span class="info-label font-medium">{{$t('dateFormat')}}</span>
        <span class="info-value">{{table.format}}</span>
      </div>
    </div>
    <div class="hr inner"></div>
    <div class="info-group">
      <div class="info-row">
        <span class="info-label font-medium">{{$t('storageType')}}</span>
        <span class="info-value" v-if="table.storageType">{{$t('kylinLang.dataSource.' + table.storageType)}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
      <div class="info-row">
        <span class="info-label font-medium">{{$t('storageSize')}}</span>
        <span class="info-value" v-if="table.storageSize !== null">{{table.storageSize | dataSize}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
      <div class="info-row" v-if="!~['incremental'].indexOf(table.storageType)">
        <span class="info-label font-medium">{{$t('totalRecords')}}</span>
        <span class="info-value" v-if="table.storageType">{{table.totalRecords}} {{$t('rows')}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
    </div>
    <template v-if="table.partitionColumn">
      <div class="hr inner"></div>
      <div class="info-group">
      <div class="info-row">
        <span class="info-label font-medium">{{$t('loadRange')}}</span>
      </div>
      <div class="info-row">
        <span class="info-label font-medium">{{$t('kylinLang.common.startTime1')}}</span>
        <span class="info-value" v-if="table.startTime !== undefined">{{table.startTime | toServerGMTDate}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
      <div class="info-row">
        <span class="info-label font-medium">{{$t('kylinLang.common.endTime1')}}</span>
        <span class="info-value" v-if="table.endTime !== undefined">{{table.endTime | toServerGMTDate}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
    </div>
    </template>
    <div class="hr"></div>
    <div class="ksd-mt-15 ksd-ml-10 ky-no-br-space">
      <el-button type="primary" size="medium" v-if="(~['incremental', 'full'].indexOf(table.storageType) || table.partitionColumn)&&isShowLoadData" @click="handleLoadData()" v-guide.tableLoadDataBtn>{{$t('loadData')}}</el-button>
      <el-button v-if="~['incremental'].indexOf(table.storageType) || table.partitionColumn" size="medium" @click="handleRefreshData">{{$t('refreshData')}}</el-button>
    </div>
    <el-dialog class="source-table-modal" width="660px"
      :title="$t('partitionSetting')"
      :visible="partitionSettingVisible"
      :close-on-click-modal="false"
      :close-on-press-escape="false"
      @close="handleClose">
      <el-form :model="form" :rules="rules" ref="form" size="medium" label-position="top">
        <el-form-item :label="$t('partitionKey')" prop="partitionColumn">
          <el-select
            v-guide.tablePartitionColumn
            filterable
            size="medium"
            v-model="form.partitionColumn">
            <el-option :label="$t('noPartition')" value=""></el-option>
            <el-option
              v-for="item in table.dateTypeColumns"
              :key="item.name"
              :label="item.name"
              :value="item.name">
              <span style="float: left">{{ item.name }}</span>
              <span class="ky-option-sub-info">{{ item.datatype }}</span>
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('dateFormat')" prop="format" v-if="form.partitionColumn">
          <el-select
            filterable
            size="medium"
            :disabled="isLoadingFormat"
            :placeholder="$t('kylinLang.common.pleaseSelect')"
            v-model="form.format">
            <el-option
              v-for="item in dateFormats"
              :key="item.label"
              :label="item.label"
              :value="item.value">
            </el-option>
          </el-select><el-tooltip effect="dark" :content="$t('detectFormat')" placement="top">
            <div style="display: inline-block;">
              <el-button
                size="small"
                class="ksd-ml-10"
                :loading="isLoadingFormat"
                v-guide.getPartitionColumnFormat
                v-if="form.partitionColumn&&$store.state.project.projectPushdownConfig"
                icon="el-icon-ksd-data_range_search"
                @click="handleLoadFormat">
              </el-button>
            </div>
          </el-tooltip>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer ky-no-br-space">
        <el-button plain size="medium" @click="handleClose">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button size="medium" @click="handleChangePartition" :disabled="!isEditPartition" :loading="isLoading">{{$t('kylinLang.common.submit')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { _getPartitionInfo, _getFullLoadInfo, _getRefreshFullLoadInfo } from './handler'
import { handleSuccessAsync, handleError, objectClone } from '../../../../util'
import { getAffectedModelsType } from '../../../../config'

@Component({
  props: {
    project: {
      type: Object
    },
    table: {
      type: Object
    },
    isShowLoadData: {
      type: Boolean,
      default: true
    }
  },
  methods: {
    ...mapActions('SourceTableModal', {
      callSourceTableModal: 'CALL_MODAL'
    }),
    ...mapActions({
      saveTablePartition: 'SAVE_TABLE_PARTITION',
      fetchRelatedModels: 'FETCH_RELATED_MODELS',
      fetchChangeTypeInfo: 'FETCH_CHANGE_TYPE_INFO',
      fetchRelatedModelStatus: 'FETCH_RELATED_MODEL_STATUS',
      fetchFullLoadInfo: 'FETCH_FULL_LOAD_INFO',
      fetchFreshInfo: 'FETCH_RANGE_FRESH_INFO',
      freshDataRange: 'FRESH_RANGE_DATA',
      fetchPartitionFormat: 'FETCH_PARTITION_FORMAT'
    })
  },
  locales
})
export default class TableDataLoad extends Vue {
  isLoadingFormat = false
  partitionSettingVisible = false
  isLoading = false
  form = {
    partitionColumn: '',
    format: ''
  }
  dateFormats = [
    {label: 'yyyy-MM-dd', value: 'yyyy-MM-dd'},
    {label: 'yyyyMMdd', value: 'yyyyMMdd'},
    {label: 'yyyy-MM-dd HH:mm:ss', value: 'yyyy-MM-dd HH:mm:ss'},
    {label: 'yyyy-MM-dd HH:mm:ss.SSS', value: 'yyyy-MM-dd HH:mm:ss.SSS'},
    {label: 'yyyy/MM/dd', value: 'yyyy/MM/dd'}
    // {label: 'yyyy-MM', value: 'yyyy-MM'},
    // {label: 'yyyyMM', value: 'yyyyMM'}
  ]
  get rules () {
    return {
      partitionColumn: [{ message: this.$t('kylinLang.common.pleaseSelect'), trigger: 'blur', required: true }],
      format: [{ message: this.$t('kylinLang.common.pleaseSelect'), trigger: 'blur', required: true }]
    }
  }
  editPartition () {
    this.form.partitionColumn = objectClone(this.table.partitionColumn)
    this.form.format = objectClone(this.table.format) || 'yyyy-MM-dd'
    this.$nextTick(() => {
      this.partitionSettingVisible = true
    })
  }
  handleClose () {
    this.partitionSettingVisible = false
  }
  get isEditPartition () {
    const oldForm = {
      partitionColumn: this.table.partitionColumn,
      format: this.table.format
    }
    return JSON.stringify(this.form) !== JSON.stringify(oldForm)
  }
  async handleLoadFormat () {
    try {
      this.isLoadingFormat = true
      const res = await this.fetchPartitionFormat({ project: this.project.name, table: this.table.fullName, partition_column: this.form.partitionColumn })
      const data = await handleSuccessAsync(res)
      this.form.format = data
      this.isLoadingFormat = false
    } catch (e) {
      this.isLoadingFormat = false
      handleError(e)
    }
  }
  async handleLoadData (isChangePartition) {
    try {
      if (isChangePartition) {
        delete this.table.startTime // 切换partition key的时候选择范围设置为空
        delete this.table.endTime
      }
      const { project, table } = this
      if (table.partitionColumn) {
        const rangeInfoTip = isChangePartition ? this.$t('suggestSetLoadRangeTip') + '<br/>' + this.$t('kylinLang.dataSource.rangeInfoTip') : this.$t('kylinLang.dataSource.rangeInfoTip')
        const isSubmit = await this.callSourceTableModal({ editType: 'loadData', project, table, format: this.table.format, rangeInfoTip: rangeInfoTip })
        isSubmit && this.$emit('fresh-tables')
      } else {
        await this.handleLoadFullData()
        this.$emit('fresh-tables')
      }
    } catch (e) {
      handleError(e)
    }
  }
  async handleRefreshData () {
    const { project, table } = this
    const isSubmit = await this.callSourceTableModal({ editType: 'refreshData', project, table, format: this.table.format })
    isSubmit && this.$emit('fresh-tables')
  }
  async handleLoadFullData () {
    const { modelCount, modelSize } = await this._getAffectedModelCountAndSize(getAffectedModelsType.RELOAD_ROOT_FACT)
    if (modelCount || modelSize) {
      await this._showFullDataLoadConfirm({ modelSize })
    }
    const submitData = _getFullLoadInfo(this.project, this.table)
    const response = await this.fetchFreshInfo(_getRefreshFullLoadInfo(this.project, this.table))
    const result = await handleSuccessAsync(response)
    submitData.affected_start = result.affected_start
    submitData.affected_end = result.affected_end
    await this.freshDataRange(submitData)
    this.$emit('fresh-tables')
    this.$message({ type: 'success', message: this.$t('loadSuccessTip') })
  }
  async handleChangePartition () {
    try {
      const { modelCount, modelSize } = await this._getAffectedModelCountAndSize(getAffectedModelsType.TOGGLE_PARTITION)
      if (modelCount || modelSize) {
        await this._showPartitionConfirm({ modelSize, partitionKey: this.form.partitionColumn })
      }
      await this._changePartitionKey(this.form.partitionColumn, this.form.format)
      this.partitionSettingVisible = false
      if (this.form.partitionColumn) {
        this.table.partitionColumn = this.form.partitionColumn
        await this.handleLoadData(true)
      }
      this.$emit('fresh-tables')
    } catch (e) {
      handleError(e)
    }
  }
  _showFullDataLoadConfirm ({ modelSize }) {
    const storageSize = Vue.filter('dataSize')(modelSize)
    const tableName = this.table.name
    const contentVal = { tableName, storageSize }
    const confirmTitle = this.$t('fullLoadDataTitle')
    const confirmMessage1 = this.$t('fullLoadDataContent1', contentVal)
    const confirmMessage2 = this.$t('fullLoadDataContent2', contentVal)
    const confirmMessage = _render(this.$createElement)
    const confirmButtonText = this.$t('kylinLang.common.submit')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const type = 'warning'
    return this.$confirm(confirmMessage, confirmTitle, { confirmButtonText, cancelButtonText, type })

    function _render (h) {
      return (
        <div>
          <p class="break-all">{confirmMessage1}</p>
          <p>{confirmMessage2}</p>
        </div>
      )
    }
  }
  _showPartitionConfirm ({ modelSize, partitionKey }) {
    const storageSize = Vue.filter('dataSize')(modelSize)
    const tableName = this.table.fullName
    const oldPartitionKey = this.table.partitionColumn || this.$t('noPartition')
    const newPartitionKey = partitionKey || this.$t('noPartition')
    const confirmTitle = this.$t('changePartitionTitle')
    const contentVal = { tableName, newPartitionKey, oldPartitionKey, storageSize }
    const confirmMessage1 = this.$t('changePartitionContent1', contentVal)
    const confirmMessage2 = partitionKey ? this.$t('changePartitionContent2', contentVal) : this.$t('changePartitionContent21', contentVal)
    const confirmMessage3 = this.$t('changePartitionContent3', contentVal)
    const confirmMessage = _render(this.$createElement)
    const confirmButtonText = partitionKey ? this.$t('submitAndSetRange') : this.$t('kylinLang.common.submit')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const type = 'warning'
    return this.$confirm(confirmMessage, confirmTitle, { confirmButtonText, cancelButtonText, type })

    function _render (h) {
      return (
        <div>
          <p class="break-all">{confirmMessage1}</p>
          <p>{confirmMessage2}</p>
          <p>{confirmMessage3}</p>
        </div>
      )
    }
  }
  _changePartitionKey (column, format) {
    const submitData = _getPartitionInfo(this.project, this.table, column, format)
    return this.saveTablePartition(submitData)
  }
  async _getAffectedModelCountAndSize (affectedType) {
    const projectName = this.project.name
    const tableName = this.table.fullName
    // const isSelectFact = !this.table.__data.increment_loading
    const response = await this.fetchChangeTypeInfo({ projectName, tableName, affectedType })
    const result = await handleSuccessAsync(response)
    return { modelCount: result.models.length, modelSize: result.byte_size }
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.table-data-load {
  .info-group {
    padding: 15px 0;
    &:first-child {
      padding-top: 0;
    }
  }
  .info-label {
    display: inline-block;
    width: 116px;
  }
  .info-value.empty {
    color: @text-disabled-color;
  }
  .info-row {
    padding: 0 10px;
  }
  .info-row:not(:last-child) {
    margin-bottom: 10px;
  }
  .edit-btn {
    display: inline-block;
    height: 22px;
    width: 22px;
    background-color: @base-color-9;
    border-radius: 50%;
    color: @base-color;
    text-align: center;
    &:hover {
      background-color: @base-color;
      color: @fff;
    }
  }
  // .hr.dashed {
  //   height: 1px;
  //   border: none;
  //   background-image: linear-gradient(to right, @line-border-color 0%, @line-border-color 50%, transparent 50%);
  //   background-size: 20px 1px;
  //   background-repeat: repeat-x;
  // }
  .hr {
    height: auto;
    border-bottom: 1px solid @line-split-color;
    background-image: none;
    &.inner{
      margin: 0 10px;
    }
  }
}
</style>
