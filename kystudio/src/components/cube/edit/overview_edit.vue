<template>
 <el-table 
  :data="cubeData"
  :show-header="false"
  border class="table_margin"
  style="width: 100%">
      <el-table-column
      width="120">
      <template slot-scope="scope">
        {{$t(scope.row.label)}}
      </template>
    </el-table-column>
    <el-table-column
    prop="value">
    </el-table-column>
  </el-table>
</template>

<script>
export default {
  name: 'overview',
  props: ['cubeDesc', 'modelDesc'],
  computed: {
    cubeData: function () {
      let cubeDetail = [
        {label: 'modelName', value: this.cubeDesc.model_name},
        {label: 'cubeName', value: this.cubeDesc.name},
        {label: 'factTable', value: this.modelDesc.fact_table},
        {label: 'lookupTable', value: this.modelDesc.lookups.length},
        {label: 'dimensions', value: this.cubeDesc.dimensions.length},
        {label: 'measures', value: this.cubeDesc.measures.length}]
      return cubeDetail
    }
  },
  locales: {
    'en': {modelName: 'Model Name', cubeName: 'Cube Name', factTable: 'Fact Table', lookupTable: 'Lookup Table', dimensions: 'Dimensions', measures: 'Measures'},
    'zh-cn': {modelName: '模型名称', cubeName: 'Cube名称', factTable: '事实表', lookupTable: '维度表', dimensions: '维度', measures: '度量'}
  }
}
</script>
<style scoped>
 .table_margin {
   margin-top: 20px;
   margin-bottom: 20px;
 }
</style>
