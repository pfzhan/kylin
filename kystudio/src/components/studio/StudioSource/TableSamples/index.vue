<template>
  <div class="table-sample">
    <div class="columns-header">
      <div class="left font-medium">
        {{$t('total') + columnCount}}
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
    <el-table class="columns-body" :data="columns" border>
      <el-table-column
        sortable
        align="left"
        v-if="headers.length"
        v-for="(headerText, index) in headers"
        :key="headerText"
        :prop="String(index)"
        :min-width="getStringWidth(headerText)"
        :label="headerText">
      </el-table-column>
    </el-table>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { changeDataAxis } from '../../../../util'

@Component({
  props: {
    table: {
      type: Object
    }
  },
  locales
})
export default class TableSamples extends Vue {
  filterText = ''
  get headers () {
    return this.table.sample_rows.length ? this.table.columns
      .map(column => column.name)
      .filter(column => column.toUpperCase().includes(this.filterText.toUpperCase())) : []
  }
  get columnCount () {
    return this.columns[0] ? this.columns[0].length : 0
  }
  get columns () {
    const columns = changeDataAxis(this.table.sample_rows)
    const headerIdxs = []

    this.table.columns.forEach((column, index) => {
      if (this.headers.includes(column.name)) {
        headerIdxs.push(index)
      }
    })

    return columns.map(column => column.filter((value, index) => headerIdxs.includes(index)))
  }
  getStringWidth (string = '') {
    return 15 * string.length + 20
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.table-sample {
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
