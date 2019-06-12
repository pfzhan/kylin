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
          :placeholder="$t('kylinLang.common.pleaseFilter')">
        </el-input>
      </div>
    </div>
    <el-table class="columns-body" :data="currentColumns" @sort-change="onSortChange" border>
      <el-table-column
        type="index"
        label="ID"
        width="64"
        :index="startIndex">
      </el-table-column>
      <el-table-column
        prop="name"
        min-width="300"
        :label="$t('kylinLang.dataSource.columnName')">
      </el-table-column>
      <el-table-column
        prop="datatype"
        width="120"
        :label="$t('kylinLang.dataSource.dataType')">
      </el-table-column>
      <el-table-column
        prop="cardinality"
        sortable="custom"
        align="right"
        header-align="right"
        min-width="105"
        :label="$t('kylinLang.dataSource.cardinality')">
      </el-table-column>
      <el-table-column
        prop="min_value"
        align="right"
        header-align="right"
        min-width="105"
        :label="$t('kylinLang.dataSource.minimal')">
      </el-table-column>
      <el-table-column
        prop="max_value"
        align="right"
        header-align="right"
        min-width="105"
        :label="$t('kylinLang.dataSource.maximum')">
      </el-table-column>
      <el-table-column
        prop="comment"
        :label="$t('kylinLang.dataSource.comment')">
        <template slot-scope="scope">
          <span :title="scope.row.comment">{{scope.row.comment}}</span>
        </template>
      </el-table-column>
    </el-table>
    <kap-pager
      class="ksd-center ksd-mt-10" ref="pager"
      :totalSize="columns.length"
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
    pageOffset: 0,
    pageSize: pageCount
  }
  get startIndex () {
    const { pageOffset, pageSize } = this.pagination
    return pageOffset * pageSize + 1
  }
  get totalDataRows () {
    return this.table.totalRecords
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
