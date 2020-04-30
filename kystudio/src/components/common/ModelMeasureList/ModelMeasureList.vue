<template>
  <div class="model-measure-list">
    <div class="measure-list-header clearfix">
      <div class="ksd-fright">
        <el-input
          class="measure-filter"
          v-model.trim="filters[0].name"
          prefix-icon="el-icon-search"
          :placeholder="$t('kylinLang.common.searchMeasureName')"
        />
      </div>
    </div>
    <el-table border :data="measures.data" style="width: 100%">
      <el-table-column prop="name" :label="$t('kylinLang.cube.measure')" />
      <el-table-column prop="expression" :label="$t('kylinLang.dataSource.expression')" width="170px" />
      <el-table-column prop="parameters" :label="$t('kylinLang.model.parameters')">
        <template slot-scope="scope">
          <template v-if="scope.row.expression === 'TOP_N'">
            <div class="parameter" v-for="(parameter, idx) in scope.row.parameters" :key="`${parameter.type}.${parameter.value}`">
              <span class="parameter-label" v-if="idx === 0">{{$t('kylinLang.model.orderBy_c')}}</span>
              <span class="parameter-label" v-else>{{$t('kylinLang.model.groupBy_c')}}</span>
              <span class="parameter-value">{{parameter.value}}</span>
            </div>
          </template>
          <template v-else>
            <div class="parameter" v-for="parameter in scope.row.parameters" :key="`${parameter.type}.${parameter.value}`">
              <span class="parameter-label">{{$t('kylinLang.model.type_c')}}</span>
              <span class="parameter-type">{{parameter.type}}</span>
              <span class="parameter-label">{{$t('kylinLang.model.value_c')}}</span>
              <span class="parameter-value">{{parameter.value}}</span>
            </div>
          </template>
        </template>
      </el-table-column>
      <el-table-column prop="returnType" :label="$t('kylinLang.model.returnType')" width="160px" />
    </el-table>
    <kap-pager
      class="ksd-center ksd-mtb-10"
      layout="total, prev, pager, next"
      :totalSize="measures.totalCount"
      :curPage="measures.pageOffset + 1"
      @handleCurrentChange="value => pageOffset = value">
    </kap-pager>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import { dataHelper } from '../../../util'

@Component({
  props: {
    model: {
      type: Object
    }
  }
})
export default class ModelMeasureList extends Vue {
  pageOffset = 0
  pageSize = 10
  filters = [
    { name: '' }
  ]

  get measures () {
    const { filters, model: { measures: datas }, pageOffset, pageSize } = this
    return dataHelper.getPaginationTable({ filters, datas, pageOffset, pageSize })
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.model-measure-list {
  .measure-list-header {
    margin-bottom: 10px;
  }
  .measure-filter {
    width: 250px;
  }
  .parameter-label {
    color: @color-info;
  }
  .parameter-type:nth-child(2) + .parameter-label {
    margin-left: 5px;
  }
  .el-table__body-wrapper {
    font-size: 12px;
  }
  .el-table .cell {
    line-height: 18px;
  }
}
</style>
