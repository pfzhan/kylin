<template>
  <div class="model-dimension-list">
    <div class="list-empty" v-if="dimensions.totalCount === 0">
      <div>{{$t('kylinLang.common.noDimensionInModel')}}</div>
      <div>{{$t('kylinLang.common.pleaseClickEditModel')}}</div>
    </div>
    <template v-else>
      <div class="dimension-list-header clearfix">
        <div class="ksd-fright">
          <el-input
            class="dimension-filter"
            v-model.trim="filters[0].name"
            prefix-icon="el-icon-search"
            :placeholder="$t('kylinLang.common.searchDimensionName')"
          />
        </div>
      </div>
      <el-table border :data="dimensions.data" style="width: 100%">
        <el-table-column prop="name" :label="$t('kylinLang.dataSource.dimensionName')" />
        <el-table-column prop="table" :label="$t('kylinLang.common.tableName')" />
        <el-table-column prop="column" :label="$t('kylinLang.dataSource.columnName')" />
        <el-table-column prop="dataType" :label="$t('kylinLang.dataSource.dataType')" width="160px" />
      </el-table>
      <kap-pager
        class="ksd-center ksd-mtb-10"
        layout="total, prev, pager, next"
        :totalSize="dimensions.totalCount"
        :curPage="dimensions.pageOffset + 1"
        @handleCurrentChange="value => pageOffset = value">
      </kap-pager>
    </template>
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
export default class ModelDimensionList extends Vue {
  pageOffset = 0
  pageSize = 10
  filters = [
    { name: '' }
  ]

  get dimensions () {
    const { filters, model: { dimensions: datas }, pageOffset, pageSize } = this
    return dataHelper.getPaginationTable({ filters, datas, pageOffset, pageSize })
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.model-dimension-list {
  .list-empty {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    color: @text-secondary-color;
    text-align: center;
  }
  .dimension-list-header {
    margin-bottom: 10px;
  }
  .dimension-filter {
    width: 250px;
  }
  .el-table__body-wrapper {
    font-size: 12px;
  }
  .el-table .cell {
    line-height: 18px;
  }
}
</style>
