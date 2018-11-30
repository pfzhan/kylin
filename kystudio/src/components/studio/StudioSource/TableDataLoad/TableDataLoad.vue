<template>
  <div class="table-data-load">
    <el-row class="info-group">
      <!-- 表名 -->
      <el-row class="info-row">
        <div class="info-label font-medium">
          <span>{{$t('tableName')}}</span>&ensp;<span>{{$t(':')}}</span>
        </div>
        <div class="info-value">{{table.name}}</div>
      </el-row>
      <!-- 表类型：中心表/普通表 -->
      <el-row class="info-row">
        <div class="info-label font-medium">
          <span>{{$t('loadingType')}}</span>
          <span>{{$t(':')}}</span>
        </div>
        <div class="info-value">
          <el-tooltip effect="dark" :content="$t('incrementalDesc')" placement="top" v-model="isToolTipShow.incremental">
            <el-radio :value="isIncremental" :label="true" @click.native="event => handleChangeType(event, true)" :disabled="!partitionColumns.length">{{$t('incrementalLoading')}}</el-radio>
          </el-tooltip>
          <el-tooltip effect="dark" :content="$t('fullDesc')" placement="top" v-model="isToolTipShow.full">
            <el-radio :value="isIncremental" :label="false" @click.native="event => handleChangeType(event, false)">{{$t('fullTable')}}</el-radio>
          </el-tooltip>
        </div>
      </el-row>
    </el-row>
    <el-row class="info-group" v-if="false">
      <el-row class="info-row">
        <div class="info-label font-medium">
          <span>{{$t('storageType')}}</span>
          <span>{{$t(':')}}</span>
        </div>
        <div class="info-value">
          {{ 'Snapshot' }}
        </div>
      </el-row>
      <el-row class="info-row">
        <div class="info-label font-medium">
          <span>{{$t('schemaChanging')}}</span>
          <i class="el-icon-ksd-what" @click="isSchemaChangeShow = !isSchemaChangeShow"></i>
          <span>{{$t(':')}}</span>
        </div>
        <div class="info-value">
          <!-- <el-radio :value="isIncremental" :label="true" @click.native="handleChangeType(true)" :disabled="!partitionColumns.length">SCD1</el-radio>
          <el-radio :value="isIncremental" :disabled="!isIncremental" :label="false" @click.native="handleChangeType(false)">SCD2</el-radio> -->
        </div>
      </el-row>
      <el-collapse-transition>
        <div class="table-remind" v-show="isSchemaChangeShow">
          <div class="table-remind-row">
            <h1 class="remind-header font-medium">{{$t('scdTitle')}}</h1>
            <p>{{$t('scdDesc')}}</p>
          </div>
          <div class="table-remind-row">
            <h1 class="remind-header font-medium">{{$t('scd1Title')}}</h1>
            <p>{{$t('scd1Desc')}}</p>
          </div>
          <div class="table-remind-row">
            <h1 class="remind-header font-medium">{{$t('scd2Title')}}</h1>
            <p>{{$t('scd2Desc')}}</p>
          </div>
        </div>
      </el-collapse-transition>
    </el-row>
    <!-- v-if: 中心表才会展示的字段 -->
    <el-row class="info-group" v-if="isIncremental">
      <!-- 表的分区列 -->
      <el-row class="info-row">
        <div class="info-label font-medium">
          <span>{{$t('partition')}}</span>&ensp;<span>{{$t(':')}}</span>
        </div>
        <div class="info-value">
          <div>{{table.partitioned_column}}</div>
        </div>
      </el-row>
      <!-- Table数据区间时间选择 -->
      <el-row class="info-row data-range">
        <div class="info-label font-medium">
          <span>{{$t('dataRange')}}</span>
          <el-tooltip effect="dark" :content="$t('dataRangeTip')" placement="top">
            <i class="el-icon-ksd-what" @click="isDataRangeShow = !isDataRangeShow"></i>
          </el-tooltip>
          <span>{{$t(':')}}</span>
        </div>
        <div class="info-value">
          <div class="date-text left">{{minDataRangeStr}}</div>
          <div class="date-text right">{{maxDataRangeStr}}</div>
          <DataRangeBar
            :max-range="table.allRange"
            :value="table.userRange"
            :is-left-disable="table.isMinRangeDisabled"
            :is-right-disable="table.isMaxRangeDisabled"
            @click="handleChangeDataRange">
          </DataRangeBar>
        </div>
      </el-row>
      <!-- Table数据区间操作 -->
      <el-row class="info-row">
        <div class="info-value">
          <el-button size="small" icon="el-icon-ksd-data_range" @click="handleChangeDataRange(table.userRange)">{{$t('incrementalLoading')}}</el-button>
          <el-button size="small" icon="el-icon-ksd-table_refresh" @click="handleRefreshTable">{{$t('refreshData')}}</el-button>
          <el-button size="small" icon="el-icon-ksd-merge" @click="handleTableMerge">{{$t('mergeData')}}</el-button>
        </div>
      </el-row>
    </el-row>

    <template v-if="isFact">
      <RelatedModels
        :project="project"
        :table="table"
        :related-models="relatedModels"
        @filter="handleFilterModels"
        @load-more="handleLoadMore"/>
    </template>
  </div>
</template>

<script>
import Vue from 'vue'
import dayjs from 'dayjs'
import { mapActions } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import locales from './locales'
import DataRangeBar from '../../../common/DataRangeBar/DataRangeBar'
import RelatedModels from '../RelatedModels/RelatedModels'
import { getAvailableOptions } from '../../../../util/specParser'
import { handleSuccessAsync, handleError, isDatePartitionType } from '../../../../util'

@Component({
  props: {
    project: {
      type: Object
    },
    table: {
      type: Object
    }
  },
  components: {
    DataRangeBar,
    RelatedModels
  },
  methods: {
    ...mapActions('SourceTableModal', {
      callSourceTableModal: 'CALL_MODAL'
    }),
    ...mapActions({
      saveIncrementalTable: 'SAVE_FACT_TABLE',
      fetchRelatedModels: 'FETCH_RELATED_MODELS',
      fetchChangeTypeInfo: 'FETCH_CHANGE_TYPE_INFO'
    })
  },
  locales
})
export default class TableDataLoad extends Vue {
  pagination = {
    pageOffset: 0,
    pageSize: 50
  }
  filterModelName = null
  relatedModels = []
  isSchemaChangeShow = true
  isDataRangeShow = false
  isToolTipShow = {
    incremental: false,
    full: false
  }
  get minDataRangeStr () {
    return dayjs(this.table.allRange[0]).format('YYYY-MM-DD')
  }
  get maxDataRangeStr () {
    return dayjs(this.table.allRange[1]).format('YYYY-MM-DD')
  }
  get isIncremental () {
    return this.table.fact
  }
  get isFact () {
    return this.table.root_fact
  }
  get partitionColumns () {
    return this.table.columns.filter(column => isDatePartitionType(column.datatype))
  }
  get projectName () {
    return this.project.name
  }
  mounted () {
    if (this.isFact) {
      this.loadRelatedModel()
    }
  }
  @Watch('table')
  onTableChange () {
    this.loadRelatedModel()
  }
  async handleChangeDataRange (newDataRange) {
    const isSubmit = await this.callSourceTableModal({ editType: 'changeDataRange', table: this.table, newDataRange })
    if (isSubmit) {
      this.$emit('fresh-tables')
    } else {
      this.table.allRange = [...this.table.allRange]
      this.table.userRange = [...this.table.userRange]
    }
  }
  hideTooltip () {
    for (const key in this.isToolTipShow) {
      this.isToolTipShow[key] = true
      setTimeout(() => {
        this.isToolTipShow[key] = false
      })
    }
  }
  async handleChangeType (event, value) {
    event.preventDefault()
    try {
      if (value !== this.isIncremental) {
        const { modelCount, modelSize } = await this.getAffectedModelCountAndSize(value)

        if (modelCount || modelSize) {
          await this.showUserConfirm({ modelCount, modelSize })
          this.hideTooltip()  // fix el-tooltip bug
          await this.setChangeTableType(value)
        } else if (this.partitionColumns.length) {
          await this.setChangeTableType(value)
        }
      }
    } catch (e) {
      handleError(e)
    }
  }
  async handleRefreshTable () {
    const isSubmit = await this.callSourceTableModal({ editType: 'refreshData', table: this.table })
    isSubmit && this.$emit('fresh-tables')
  }
  async handleTableMerge () {
    const { table, projectName } = this
    const isSubmit = await this.callSourceTableModal({ editType: 'dataMerge', table, projectName })
    isSubmit && this.$emit('fresh-tables')
  }
  async handleLoadMore () {
    try {
      await this.loadRelatedModel({ isReset: false })
    } catch (e) {
      e && handleError(e)
    }
  }
  async handleFilterModels (value) {
    try {
      this.filterModelName = value
      this.loadRelatedModel()
    } catch (e) {
      e && handleError(e)
    }
  }
  addPagination () {
    this.pagination.pageOffset++
  }
  clearPagination () {
    this.pagination.pageOffset = 0
  }
  async loadRelatedModel (options) {
    const { isReset = true } = options || {}
    const { projectName, table, pagination } = this
    const tableFullName = `${table.database}.${table.name}`
    const modelName = this.filterModelName
    const res = await this.fetchRelatedModels({ projectName, tableFullName, modelName, ...pagination })
    const { size, models } = await handleSuccessAsync(res)
    const formatedModels = this.formatModelData(models)
    if (isReset) {
      this.relatedModels = []
    }
    if (size > this.relatedModels.length) {
      if (isReset) {
        this.relatedModels = formatedModels
        this.clearPagination()
      } else {
        this.relatedModels = [ ...this.relatedModels, ...formatedModels ]
        this.addPagination()
      }
    }
  }
  async getAffectedModelCountAndSize (isSelectFact) {
    let modelCount = 0
    let modelSize = 0
    try {
      const projectName = this.projectName
      const tableName = `${this.table.database}.${this.table.name}`
      const response = await this.fetchChangeTypeInfo({ projectName, tableName, isSelectFact })
      const result = await handleSuccessAsync(response)
      modelCount = result.models.length
      modelSize = result.byte_size
    } catch (e) {}

    return { modelCount, modelSize }
  }
  async showUserConfirm ({ modelCount, modelSize }) {
    const storageSize = Vue.filter('dataSize')(modelSize)
    const confirmTitle = this.$t('kylinLang.common.notice')
    const confirmMessage = this.$t('changeTableTypeCost', { modelCount, storageSize })
    const confirmButtonText = this.isIncremental ? this.$t('confirmToFull') : this.$t('confirmToIncremental')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const type = 'warning'
    await this.$confirm(confirmMessage, confirmTitle, { confirmButtonText, cancelButtonText, type })
  }
  async setChangeTableType (isSetIncremental) {
    if (isSetIncremental) {
      const isSubmit = await this.callSourceTableModal({ editType: 'changeTableType', table: this.table })
      isSubmit && this.$emit('fresh-tables')
    } else {
      const projectName = this.projectName
      const tableFullName = `${this.table.database}.${this.table.name}`
      const isIncremental = false
      await this.saveIncrementalTable({ projectName, tableFullName, isIncremental })
      this.$emit('fresh-tables')
    }
  }
  formatModelData (models) {
    return models.map(model => {
      let startTime = Infinity
      let endTime = -Infinity
      let isOnline = true
      const segments = Object.entries(model.segment_ranges).map(([key, value]) => {
        const [ startTime, endTime ] = key.replace(/^TimePartitionedSegmentRange\[|\)$/g, '').split(',')
        return { startTime, endTime, status: value }
      })
      segments.forEach(segment => {
        segment.startTime < startTime && (startTime = +segment.startTime)
        segment.endTime > endTime && (endTime = +segment.endTime)
        segment.status === 'NEW' && (isOnline = false)
      })
      const projectType = this.project.maintain_model_type
      const modelType = model.management_type
      const modelActions = getAvailableOptions('modelActions', { projectType, modelType })
      return { ...model, segments, startTime, endTime, isOnline, modelActions }
    })
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.table-data-load {
  // height: calc(~'100vh - 249px');
  overflow: visible;
  box-sizing: border-box;
  .range-submit {
    margin-left: 3px;
  }
  .info-group {
    margin: 15px 0;
    &:first-child {
      margin-top: 5px;
    }
  }
  .info-row {
    font-size: 14px;
    line-height: 1;
    color: #263238;
    margin-bottom: 15px;
    &:last-child {
      margin-bottom: 0;
    }
  }
  .info-label {
    width: 150px;
    float: left;
    text-align: right;
    white-space: nowrap;
    &.small {
      width: 110px;
      white-space: nowrap;
    }
  }
  .el-icon-ksd-what {
    margin-left: 1px;
    margin-right: 1px;
    position: relative;
  }
  .info-value {
    margin-left: 160px;
  }
  .info-row.range-process .info-label {
    position: relative;
    transform: translateY(-3px);
  }
  .info-row.range-process .info-value {
    padding: 0 6px;
  }
  .table-remind {
    padding: 20px;
    font-size: 12px;
    background: #E6F3FC;
    box-sizing: border-box;
    p {
      line-height: 18px;
    }
  }
  .table-remind-row {
    margin-bottom: 10px;
    &:last-child {
      margin-bottom: 0;
    }
  }
  .remind-header {
    font-size: 14px;
    margin-bottom: 10px;
  }
  .data-range {
    .info-value {
      padding-top: 10px;
      position: relative;
      width: 70%;
    }
    .date-text {
      position: absolute;
      top: 0;
    }
    .left {
      left: 0;
    }
    .right {
      right: 0;
    }
  }
  .status {
    margin-left: 10px;
  }
  .date-range-input {
    .el-flex-box {
      width: 100%;
    }
    margin-left: 10px;
  }
}
</style>
