<template>
  <div class="table-data-load">
    <div class="table-info">
      <!-- 表名 -->
      <div class="info-row">
        <div class="info-label font-medium">
          <span>{{$t('tableName')}}</span><span>:</span>
        </div>
        <div class="info-value">{{table.name}}</div>
      </div>
      <!-- 表类型：中心表/普通表 -->
      <div class="info-row">
        <div class="info-label font-medium">
          <span>{{$t('tableType')}}</span><span>:</span>
        </div>
        <div class="info-value">
          <div>{{tableType}}</div>
        </div>
      </div>
      <!-- v-if: 中心表才会展示的字段 -->
      <template v-if="isCentral">
        <!-- 表的分区列 -->
        <div class="info-row">
          <div class="info-label font-medium">
            <span>{{$t('partition')}}</span><span>:</span>
          </div>
          <div class="info-value">
            <div>{{table.partitioned_column}}</div>
          </div>
        </div>
        <!-- 标题 -->
        <div class="info-row clearfix">
          <div class="info-label font-medium">
            <span>{{$t('dataRange')}}</span>
            <i class="el-icon-ksd-what"></i>
          </div>
        </div>
        <!-- 时间区间 -->
        <div class="info-row date-range-box">
          <!-- 数据加载区间 -->
          <div class="row-item">
            <div class="info-label font-medium small">
              <span>{{$t('loadingRange')}}</span>
              <span>:</span>
            </div>
            <div class="load-range">
              <DateRangeBar :date-ranges="tableDateRange" :is-show-start-to-end="true" />
            </div>
          </div>
          <!-- 选择时间区间 -->
          <div class="row-item">
            <div class="info-label font-medium small">
              <span>{{$t('dateRange')}}</span>
              <i class="el-icon-ksd-what"></i>
              <span>:</span>
            </div>
            <el-date-picker
              size="medium"
              class="date-range-input"
              v-model="dateRange"
              type="datetimerange"
              range-separator="-"
              :start-placeholder="$t('startTime')"
              :end-placeholder="$t('endTime')"
              :picker-options="{ disabledDate: getDisabledDate }">
            </el-date-picker>
            <el-button size="medium" type="primary" @click="handleSubmitRange">{{$t('kylinLang.common.submit')}}</el-button>
          </div>
        </div>
      </template>
    </div>

    <template v-if="isCentral">
      <h1 class="related-model-title font-medium">{{$t('relatedModel')}}</h1>
      <el-table class="table" :data="relatedModels" border>
        <el-table-column
          prop="alias"
          :label="$t('modelName')"
          width="275px"
          sortable
          align="center">
        </el-table-column>
        <el-table-column
          prop="progress"
          :label="$t('status')"
          align="center">
          <template slot-scope="scope">
            <div class="load-range">
              <DateRangeBar :date-ranges="scope.row.dataRanges" />
            </div>
            <span class="status" v-if="isDateRangeReady(scope.row.dataRanges, 'model')">Ready</span>
            <span class="status" v-else>In Progress</span>
          </template>
        </el-table-column>
      </el-table>
      <kap-pager
        class="ksd-center ksd-mt-20 ksd-mb-20" ref="pager"
        :totalSize="relatedModels.length"
        @handleCurrentChange="handlePagination">
      </kap-pager>
    </template>

    <div class="table-remind">
      <div class="table-remind-row">
        <h1 class="remind-header font-medium">{{$t('centralTableTitle')}}</h1>
        <p>{{$t('centralTableDesc')}}</p>
      </div>
      <div class="table-remind-row">
        <h1 class="remind-header font-medium">{{$t('normalTableTitle')}}</h1>
        <p>{{$t('normalTableDesc')}}</p>
      </div>
    </div>

  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import locales from './locales'
import DateRangeBar from '../../../common/DateRangeBar/index.vue'
import { handleSuccessAsync } from '../../../../util'
import { getModelDataRanges, getTableDataRanges } from './handle'

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
    DateRangeBar
  },
  methods: {
    ...mapActions({
      saveDateRange: 'SAVE_DATE_RANGE',
      fetchRelatedModels: 'FETCH_RELATED_MODELS'
    })
  },
  locales
})
export default class TableDataLoad extends Vue {
  startDate = ''
  endDate = ''
  pagination = {
    pageOffset: 0,
    pageSize: 50
  }
  tableDateRange = []
  relatedModels = []
  get isCentral () {
    return this.table.fact
  }
  get isFact () {
    return this.table.root_fact
  }
  get tableType () {
    return this.isCentral ? this.$t('centralTable') : this.$t('normalTable')
  }
  get dateRange () {
    return [this.startDate, this.endDate]
  }
  set dateRange (val) {
    if (val) {
      const [startDate, endDate] = val
      this.startDate = startDate
      this.endDate = endDate
    } else {
      this.startDate = ''
      this.endDate = ''
    }
  }
  isDateRangeReady (dateRanges, type) {
    return dateRanges.every((dateRange) => dateRange.status === 'READY')
  }
  mounted () {
    if (this.isCentral) {
      this.resetDateRange()
      this.getRelatedModel()
    }
  }
  @Watch('table')
  async onTableChange (table) {
    if (this.isCentral) {
      await this.getRelatedModel()
      this.resetDateRange()
    }
  }
  async handleSubmitRange () {
    const projectName = this.project.name
    const tableFullName = `${this.table.database}.${this.table.name}`
    const startDate = this.startDate.getTime()
    const endDate = this.endDate.getTime()

    if (this.isDateRangeVaild()) {
      await this.saveDateRange({ projectName, tableFullName, startDate, endDate })
      this.$message('更新成功。')
    } else {
      this.$message('不可选择比数据表更小的时间区间。')
    }

    this.$emit('on-data-range-change')
  }
  handlePagination (val) {
    this.pagination.pageOffset = val - 1
  }
  resetDateRange () {
    this.tableDateRange = getTableDataRanges(this.table, this.relatedModels) || []
    this.startDate = new Date(this.table.start_time)
    this.endDate = new Date(this.table.end_time)
  }
  async getRelatedModel () {
    const projectName = this.project.name
    const tableFullName = `${this.table.database}.${this.table.name}`

    const res = await this.fetchRelatedModels({ projectName, tableFullName })
    const { models } = await handleSuccessAsync(res)
    this.relatedModels = models.map(model => {
      model.dataRanges = getModelDataRanges(model)
      return model
    })
  }
  getDisabledDate (time) {
    return this.table.start_time < time.getTime() &&
      time.getTime() < this.table.end_time
  }
  isDateRangeVaild () {
    const startTime = this.startDate.getTime()
    const endTime = this.endDate.getTime()
    const tableStartTime = this.table.start_time
    const tableEndTime = this.table.end_time

    return startTime <= tableStartTime && endTime >= tableEndTime
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.table-data-load {
  // height: calc(~'100vh - 249px');
  overflow: auto;
  box-sizing: border-box;
  .table-info {
    background: #f7f7f7;
    padding: 20px;
  }
  .range-submit {
    margin-left: 3px;
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
    width: 97px;
    float: left;
    text-align: right;
    span:last-child {
      margin-left: 8px;
    }
    &.small {
      width: 110px;
      white-space: nowrap;
    }
  }
  .el-icon-ksd-what {
    margin-left: 3px;
    margin-right: -3px;
    position: relative;
    transform: translateY(1px);
  }
  .info-value {
    margin-left: 120px;
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
    margin-top: 20px;
    font-size: 12px;
    background: #E6F3FC;
  }
  .table-remind-row {
    margin-bottom: 20px;
    &:last-child {
      margin-bottom: 0;
    }
  }
  .remind-header {
    font-size: 14px;
    margin-bottom: 10px;
  }
  .load-range {
    width: calc(~'85% - 140px');
    display: inline-block;
    margin-left: 15px;
    .progress-bar {
      width: 100%;
    }
  }
  .status {
    margin-left: 10px;
  }
  .table {
    width: 100%;
    .progress-bar {
      width: calc(~'100% - 120px');
    }
  }
  .related-model-title {
    font-size: 16px;
    color: #263238;
    margin: 25px 0 10px 0;
  }
  .el-table__row > td {
    text-align: left;
  }
  .date-range-box {
    background: white;
    padding: 10px 10px 20px 10px;
  }
  .row-item {
    margin-bottom: 35px;
    &:last-child {
      margin-bottom: 0px;
      .info-label {
        position: relative;
        top: 9px;
      }
    }
  }
  .date-range-input {
    .el-flex-box {
      width: 100%;
    }
    margin-left: 10px;
  }
}
</style>
