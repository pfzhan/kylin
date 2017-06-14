<template>
  <div id="over-view">
    <el-table class="table_margin"
      :data="cubeData"
      :show-header="false"
      border
      style="width: 100%">
        <el-table-column width="180">
          <template scope="scope">
            {{$t(scope.row.label)}}
          </template>
        </el-table-column>
        <el-table-column
        prop="value">
        </el-table-column>
    </el-table>
  </div>
</template>

<script>
export default {
  name: 'overview',
  props: ['desc'],
  computed: {
    cubeData: function () {
      let cubeDetail = [
        {label: 'modelName', value: this.desc.model},
        {label: 'cubeName', value: this.desc.name},
        {label: 'factTable', value: this.desc.modelDesc.fact_table},
        {label: 'lookupTable', value: this.desc.modelDesc.lookups.length},
        {label: 'dimensions', value: this.desc.modelDesc.dimensions.length},
        {label: 'measures', value: this.desc.modelDesc.metrics.length}]
      return cubeDetail
    }
  },
  locales: {
    'en': {modelName: 'Model Name', cubeName: 'Cube Name', factTable: 'Fact Table', lookupTable: 'Lookup Table', dimensions: 'Dimensions', measures: 'Measures'},
    'zh-cn': {modelName: '模型名称', cubeName: 'Cube名称', factTable: '事实表', lookupTable: '维度表', dimensions: '维度', measures: '度量'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .table_margin {
    margin-top: 20px;
    margin-bottom: 20px;
  }
  #over-view{
    .el-card{
      border-color: @grey-color;
    }
  }
</style>
