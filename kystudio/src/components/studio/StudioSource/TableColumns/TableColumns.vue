<template>
  <div class="table-columns">
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
        width="64"
        header-align="center"
        :index="startIndex">
      </el-table-column>
      <el-table-column
        prop="name"
        sortable
        align="left"
        min-width="300"
        header-align="center"
        :label="$t('kylinLang.dataSource.columnName')">
      </el-table-column>
      <el-table-column
        prop="dataType"
        sortable
        align="left"
        width="120"
        header-align="center"
        :label="$t('kylinLang.dataSource.dataType')">
      </el-table-column>
      <el-table-column
        prop="cardinality"
        sortable
        align="left"
        min-width="105"
        header-align="center"
        :label="$t('kylinLang.dataSource.cardinality')">
      </el-table-column>
      <el-table-column
        prop="comment"
        sortable
        align="left"
        min-width="200"
        header-align="center"
        :label="$t('kylinLang.dataSource.comment')">
        <template slot-scope="scope">
          <span :title="scope.row.comment">{{scope.row.comment}}</span>
        </template>
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
  get columns () {
    return this.table.columns
      .filter(column => column.name.toUpperCase().includes(this.filterText.toUpperCase()))
  }
  get currentColumns () {
    const { pageOffset, pageSize } = this.pagination
    return this.columns.slice(pageOffset * pageSize, pageOffset * pageSize + pageSize).map(column => {
      const cardinality = this.getCardinality(column.name)
      return { ...column, cardinality }
    })
  }
  getCardinality (columnName) {
    return this.table.cardinality[columnName]
  }
  handleCurrentChange (pageOffset, pageSize) {
    this.pagination.pageOffset = pageOffset
    this.pagination.pageSize = pageSize
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.table-columns {
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
