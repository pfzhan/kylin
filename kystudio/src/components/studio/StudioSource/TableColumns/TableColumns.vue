<template>
  <div class="table-columns">
    <div class="columns-header">
      <div class="left font-medium">
        <span v-if="totalDataRows">{{$t('total') + totalDataRows}}</span>
      </div>
      <div class="right">
        <el-input
          class="filter-input"
          prefix-icon="el-icon-search"
          v-model="filterText"
          @input="filterChange"
          :placeholder="$t('kylinLang.common.pleaseFilter')">
        </el-input>
      </div>
    </div>
    <el-table class="columns-body" :data="currentColumns" @sort-change="onSortChange" border>
      <el-table-column
        type="index"
        label="ID"
        width="64"
        show-overflow-tooltip
        :index="startIndex">
      </el-table-column>
      <el-table-column
        prop="name"
        min-width="300"
        show-overflow-tooltip
        :label="$t('kylinLang.dataSource.columnName')">
      </el-table-column>
      <el-table-column
        prop="datatype"
        width="120"
        show-overflow-tooltip
        :label="$t('kylinLang.dataSource.dataType')">
      </el-table-column>
      <el-table-column
        prop="cardinality"
        sortable="custom"
        align="right"
        header-align="right"
        min-width="105"
        show-overflow-tooltip
        :label="$t('kylinLang.dataSource.cardinality')">
      </el-table-column>
      <el-table-column
        prop="min_value"
        align="right"
        header-align="right"
        min-width="105"
        show-overflow-tooltip
        :label="$t('kylinLang.dataSource.minimal')">
      </el-table-column>
      <el-table-column
        prop="max_value"
        align="right"
        header-align="right"
        min-width="105"
        show-overflow-tooltip
        :label="$t('kylinLang.dataSource.maximum')">
      </el-table-column>
      <el-table-column
        prop="comment"
        show-overflow-tooltip
        :label="$t('kylinLang.dataSource.comment')">
        <template slot-scope="scope">
          <span :title="scope.row.comment">{{scope.row.comment}}</span>
        </template>
      </el-table-column>
    </el-table>
    <kap-pager
      class="ksd-center ksd-mt-10" ref="pager"
      :totalSize="columns.length"
      :curPage="pagination.page_offset + 1"
      @handleCurrentChange="handleCurrentChange">
    </kap-pager>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { pageCount } from '../../../../config'

@Component({
  props: {
    table: {
      type: Object
    }
  },
  locales
})
export default class TableColumns extends Vue {
  filterText = ''
  pagination = {
    page_offset: 0,
    pageSize: pageCount
  }
  get startIndex () {
    const { page_offset, pageSize } = this.pagination
    return page_offset * pageSize + 1
  }
  get totalDataRows () {
    return this.table.totalRecords
  }
  get columns () {
    return this.table.columns
      .filter(column => column.name.toUpperCase().includes(this.filterText.toUpperCase()))
  }
  get currentColumns () {
    const { page_offset, pageSize } = this.pagination
    return this.columns.slice(page_offset * pageSize, page_offset * pageSize + pageSize)
  }
  filterChange () {
    this.pagination.page_offset = 0
  }
  handleCurrentChange (page_offset, pageSize) {
    this.pagination.page_offset = page_offset
    this.pagination.pageSize = pageSize
  }
  onSortChange ({ column, prop, order }) {
    if (order === 'ascending') {
      this.table.columns.sort((a, b) => {
        return a[prop] - b[prop]
      })
    } else {
      this.table.columns.sort((a, b) => {
        return b[prop] - a[prop]
      })
    }
    this.handleCurrentChange(0, this.pagination.pageSize)
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.table-columns {
  padding: 0 0 20px 0;
  .columns-header {
    margin-bottom: 10px;
    white-space: nowrap;
  }
  .columns-body {
    width: 100%;
  }
  .left, .right {
    display: inline-block;
    vertical-align: bottom;
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
}
</style>
