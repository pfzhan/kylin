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
      <!-- 表类型/事实表 -->
      <div class="info-row">
        <div class="info-label font-medium">
          <span>{{$t('tableType')}}</span><span>:</span>
        </div>
        <div class="info-value">
          <div>{{table.table_type}}</div>
          <div class="table-remind">
            <el-checkbox class="font-medium" @input="handleToggleFact" :value="isFactTable">{{$t('factTableCheck')}}</el-checkbox>
            <div class="remind-header font-medium">{{$t('factTableTitle')}}</div>
            <div class="remind-tip">
              <b class="font-medium">{{$t('factTableName')}}</b>
              <span>{{$t('factTableTip')}}</span>
            </div>
          </div>
        </div>
      </div>
      <!-- Data Range 选择 -->
      <div class="info-row date-range">
        <div class="info-label font-medium date-label" :class="{'full-range': !isFactTable}">
          <span>{{$t('dataRange')}}</span>
          <i class="el-icon-ksd-what"></i>
          <span>:</span>
        </div>
        <div class="info-value" v-if="isFactTable">
          <el-date-picker
            :clearable="false"
            v-model="dateRange"
            type="datetimerange"
            range-separator="-"
            start-placeholder="开始日期"
            end-placeholder="结束日期">
          </el-date-picker>
          <el-button
            v-if="startDate && endDate"
            class="range-submit"
            size="medium"
            type="primary"
            @click="handleSubmitRange">
            {{$t('kylinLang.common.submit')}}
          </el-button>
        </div>
        <div class="info-value" v-if="!isFactTable">Full</div>
      </div>

      <div class="info-row range-process" v-if="isFactTable">
        <div class="info-label font-medium">
          <span>{{$t('loadRange')}}</span>
          <span>:</span>
        </div>
        <div class="info-value">
          <!-- Data Loading 展示 -->
          <div class="load-range">
            <DateRangeBar :date-ranges="dateRanges" />
          </div>
          <span class="status" v-if="false">Ready</span>
          <span class="status" v-else>In Progress</span>
        </div>
      </div>
    </div>

    <template v-if="isFactTable">
      <h1 class="related-model-title font-medium">Related Model</h1>
      <el-table class="table" :data="modelsProgress" border>
        <el-table-column
          prop="name"
          label="Model Name"
          width="275px"
          sortable
          align="center">
        </el-table-column>
        <el-table-column
          prop="progress"
          label="Status"
          align="center">
          <template slot-scope="scope">
            <div class="load-range">
              <DateRangeBar :date-ranges="dateRanges" />
            </div>
            <span class="status" v-if="scope.row.progress === 100">Ready</span>
            <span class="status" v-else>In Progress</span>
          </template>
        </el-table-column>
      </el-table>
      <kap-pager
        class="ksd-center ksd-mt-20 ksd-mb-20" ref="pager"
        :totalSize="modelsProgress.length"
        @handleCurrentChange="handlePagination">
      </kap-pager>
    </template>

  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import { getModelTableData, dateRanges } from './mock'
import locales from './locales'
import DateRangeBar from '../../../common/DateRangeBar/index.vue'

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
      saveFactTable: 'SAVE_FACT_TABLE',
      saveDateRange: 'SAVE_DATE_RANGE'
    })
  },
  locales
})
export default class TableDataLoad extends Vue {
  isFactTable = false
  startDate = ''
  endDate = ''
  pagination = {
    pageOffset: 0,
    pageSize: 50
  }
  dateRanges = dateRanges

  get dateRange () {
    return [this.startDate, this.endDate]
  }
  set dateRange ([startDate, endDate]) {
    this.startDate = startDate
    this.endDate = endDate
  }
  get modelsProgress () {
    return getModelTableData()
  }
  @Watch('table')
  onTableChange (table) {
    this.isFactTable = table.fact
  }
  async handleToggleFact (val) {
    const projectName = this.project.name
    const tableFullName = `${this.table.database}.${this.table.name}`

    await this.saveFactTable({
      project: projectName,
      table: tableFullName,
      isFact: val
    })

    this.isFactTable = !this.isFactTable
  }
  async handleSubmitRange () {
    const projectName = this.project.name
    const tableFullName = `${this.table.database}.${this.table.name}`

    await this.saveDateRange({
      project: projectName,
      table: tableFullName,
      startDate: this.startDate.getTime(),
      endDate: this.endDate.getTime()
    })
  }
  handlePagination (val) {
    this.pagination.pageOffset = val - 1
  }
  mounted () {
    this.onTableChange(this.table)
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
  }
  .info-row.date-range {
    margin-bottom: 20px;
  }
  .date-range .el-input__inner {
    width: 324px;
    height: 32px;
    line-height: 32px;
    .el-range-input {
      width: 51%;
    }
  }
  .info-row.range-process {
    margin-bottom: 25px;
  }
  .info-label {
    width: 115px;
    float: left;
    text-align: right;
    span:last-child {
      margin-left: 8px;
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
    padding: 10px;
    margin-top: 15px;
    font-size: 12px;
    background: #E6F3FC;
  }
  .remind-header {
    margin-top: 10px;
    margin-bottom: 8px;
  }
  .date-label {
    position: relative;
    top: 10px;
    &.full-range {
      top: 0;
    }
  }
  .load-range {
    width: 82%;
    display: inline-block;
    .progress-bar {
      width: calc(~'100% - 120px');
      margin-left: 20px;
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
}
</style>
