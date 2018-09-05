<template>
  <div class="table-statistics">
    <div class="columns-header">
      <div class="left font-medium">
        {{$t('total') + columns.length}}
      </div>
      <div class="right">
        <el-input
          class="filter-input"
          prefix-icon="el-icon-search"
          v-model="filterText"
          :placeholder="$t('kylinLang.common.pleaseFilter')">
        </el-input>
      </div>
    </div>
    <el-table class="columns-body" :data="currentColumns" border>
      <el-table-column
        type="index"
        label="ID"
        align="center"
        width="58px"
        :index="startIndex">
      </el-table-column>
      <el-table-column
        prop="name"
        sortable
        align="left"
        :label="$t('kylinLang.dataSource.columnName')">
      </el-table-column>
      <el-table-column
        prop="cardinality"
        sortable
        align="left"
        min-width="120px"
        :label="$t('kylinLang.dataSource.cardinality')">
      </el-table-column>
      <el-table-column
        prop="max_value"
        width="100px"
        show-overflow-tooltip
        :label="$t('kylinLang.dataSource.maximum')">
      </el-table-column>
      <el-table-column
        prop="min_value"
        show-overflow-tooltip
        width="100px"
        :label="$t('kylinLang.dataSource.minimal')">
      </el-table-column>
      <el-table-column
        show-overflow-tooltip
        prop="max_length_value"
        :label="$t('kylinLang.dataSource.maxLengthVal')">
      </el-table-column>
      <el-table-column
        show-overflow-tooltip
        prop="min_length_value"
        :label="$t('kylinLang.dataSource.minLengthVal')">
      </el-table-column>
      <el-table-column
        show-overflow-tooltip
        width="120px"
        prop="null_count"
        :label="$t('kylinLang.dataSource.nullCount')">
      </el-table-column>
      <el-table-column
        show-overflow-tooltip
        width="120px"
        prop="exceed_precision_max_length_value"
        :label="$t('kylinLang.dataSource.exceedPrecisionMaxLengthValue')">
      </el-table-column>
    </el-table>
    <kap-pager
      class="ksd-center ksd-mt-20 ksd-mb-20" ref="pager"
      :totalSize="columns.length"
      @handleCurrentChange="handleCurrentChange">
    </kap-pager>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'

@Component({
  props: {
    table: {
      type: Object
    }
  },
  locales
})
export default class TableStatistics extends Vue {
  filterText = ''
  pagination = {
    pageOffset: 0,
    pageSize: 10
  }
  get startIndex () {
    const { pageOffset, pageSize } = this.pagination
    return pageOffset * pageSize + 1
  }
  get columns () {
    return this.table.columns
      .filter(column => column.name.toUpperCase().includes(this.filterText.toUpperCase()))
  }
  get currentColumns () {
    const { pageOffset, pageSize } = this.pagination
    return this.columns.slice(pageOffset * pageSize, pageOffset * pageSize + pageSize)
  }
  handleCurrentChange (pageOffset, pageSize) {
    this.pagination.pageOffset = pageOffset
    this.pagination.pageSize = pageSize
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.table-statistics {
  padding: 0 0 20px 0;
  .columns-header {
    margin-bottom: 6px;
    white-space: nowrap;
  }
  .columns-body {
    width: 100%;
  }
  .left, .right {
    display: inline-block;
    vertical-align: middle;
    width: 49.79%;
  }
  .right {
    text-align: right;
  }
  .filter-input {
    width: 210px;
  }
  .cell {
    white-space: nowrap;
  }
  .el-pagination {
    margin-top: 30px;
  }
}
</style>
