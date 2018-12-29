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
          <el-select
            :value="table.partitionColumn"
            @input="handleChangePartition">
            <el-option :label="$t('noPartition')" value=""></el-option>
            <el-option
              v-for="item in table.dateTypeColumns"
              :key="item.name"
              :label="item.name"
              :value="item.name">
            </el-option>
          </el-select>
        </span>
      </div>
    </div>
    <div class="hr dashed"></div>
    <div class="info-group">
      <div class="info-row">
        <span class="info-label font-medium">{{$t('storageType')}}</span>
        <span class="info-value" v-if="table.storageType">{{$t('kylinLang.dataSource.' + table.storageType)}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
      <div class="info-row">
        <span class="info-label font-medium">{{$t('storageSize')}}</span>
        <span class="info-value" v-if="table.storageType">{{table.storageSize | dataSize}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
      <div class="info-row" v-if="!~['incremental'].indexOf(table.storageType)">
        <span class="info-label font-medium">{{$t('totalRecords')}}</span>
        <span class="info-value" v-if="table.storageType">{{table.totalRecords}} {{$t('rows')}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
    </div>
    <template v-if="table.partitionColumn">
      <div class="hr dashed"></div>
      <div class="info-group">
      <div class="info-row">
        <span class="info-label font-medium">{{$t('loadRange')}}</span>
      </div>
      <div class="info-row">
        <span class="info-label font-medium">{{$t('kylinLang.common.startTime1')}}</span>
        <span class="info-value" v-if="table.startTime !== undefined">{{table.startTime | utcTime}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
      <div class="info-row">
        <span class="info-label font-medium">{{$t('kylinLang.common.endTime1')}}</span>
        <span class="info-value" v-if="table.endTime !== undefined">{{table.endTime | utcTime}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
    </div>
    </template>
    <div class="hr"></div>
    <div class="ksd-mt-20">
      <el-button type="primary" v-if="~['incremental', 'full'].indexOf(table.storageType) || table.partitionColumn" @click="handleLoadData">{{$t('loadData')}}</el-button>
      <el-button v-if="~['incremental'].indexOf(table.storageType) || table.partitionColumn" @click="handleRefreshData">{{$t('kylinLang.common.refresh')}}</el-button>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { _getPartitionInfo, _getFullLoadInfo } from './handler'
import { handleSuccessAsync, handleError } from '../../../../util'

@Component({
  props: {
    project: {
      type: Object
    },
    table: {
      type: Object
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
      fetchFullLoadInfo: 'FETCH_FULL_LOAD_INFO'
    })
  },
  locales
})
export default class TableDataLoad extends Vue {
  async handleRefreshData () {
    const { project, table } = this
    const isSubmit = await this.callSourceTableModal({ editType: 'refreshData', project, table })
    isSubmit && this.$emit('fresh-tables')
  }
  async handleLoadFullData () {
    const submitData = _getFullLoadInfo(this.project, this.table)
    const { storageSize } = await this.fetchFullLoadInfo(submitData)
    await this._showAffectFullDataConfirm(storageSize)
    return await this.saveLoadRange(submitData)
  }
  async handleLoadData () {
    const { project, table } = this
    if (table.partitionColumn) {
      const isSubmit = await this.callSourceTableModal({ editType: 'loadData', project, table })
      isSubmit && this.$emit('fresh-tables')
    } else {
      await this.handleLoadFullData()
      this.$emit('fresh-tables')
    }
  }
  async handleChangePartition (value) {
    try {
      const { modelCount, modelSize } = await this._getAffectedModelCountAndSize()
      if (modelCount || modelSize) {
        await this._showAffectModelConfirm(modelCount, modelSize)
      }
      await this._changePartitionKey(value)
      await this._showDataRangeConfrim()

      this.$emit('fresh-tables')
    } catch (e) {
      handleError(e)
    }
  }
  _showAffectFullDataConfirm (size) {
    const storageSize = Vue.filter('dataSize')(size)
    const confirmTitle = this.$t('kylinLang.common.notice')
    const confirmMessage = this.$t('changePartitionCost', { storageSize })
    const confirmButtonText = this.$t('kylinLang.common.ok')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const type = 'warning'
    return this.$confirm(confirmMessage, confirmTitle, { confirmButtonText, cancelButtonText, type })
  }
  _showDataRangeConfrim () {
    const confirmTitle = this.$t('kylinLang.common.notice')
    const confirmMessage = this.$t('remindLoadRange')
    const confirmButtonText = this.$t('kylinLang.common.ok')
    const type = 'warning'
    return this.$alert(confirmMessage, confirmTitle, { confirmButtonText, type })
  }
  _showAffectModelConfirm (modelCount, modelSize) {
    const storageSize = Vue.filter('dataSize')(modelSize)
    const confirmTitle = this.$t('kylinLang.common.notice')
    const confirmMessage = this.$t('changePartitionCost', { modelCount, storageSize })
    const confirmButtonText = this.$t('kylinLang.common.ok')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const type = 'warning'
    return this.$confirm(confirmMessage, confirmTitle, { confirmButtonText, cancelButtonText, type })
  }
  _changePartitionKey (value) {
    const submitData = _getPartitionInfo(this.project, this.table, value)
    return this.saveTablePartition(submitData)
  }
  async _getAffectedModelCountAndSize () {
    const projectName = this.project.name
    const tableName = this.table.fullName
    const isSelectFact = !this.table.__data.fact
    const response = await this.fetchChangeTypeInfo({ projectName, tableName, isSelectFact })
    const result = await handleSuccessAsync(response)
    return { modelCount: result.models.length, modelSize: result.byte_size }
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.table-data-load {
  .info-group {
    padding: 20px 0;
    &:first-child {
      padding-top: 0;
    }
  }
  .info-label {
    display: inline-block;
    width: 110px;
  }
  .info-value.empty {
    color: @text-disabled-color;
  }
  .info-row {
    padding: 0 10px;
  }
  .info-row:not(:last-child) {
    margin-bottom: 15px;
  }
  .hr.dashed {
    height: 1px;
    border: none;
    background-image: linear-gradient(to right, @line-border-color 0%, @line-border-color 50%, transparent 50%);
    background-size: 20px 1px;
    background-repeat: repeat-x;
  }
  .hr {
    height: auto;
    border-bottom: 1px solid @line-border-color;
    background-image: none;
  }
}
</style>
