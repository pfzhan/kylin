<template>
  <div class="overview-edit ksd-mt-20 ksd-mrl-20">
    <table class="ksd-table ">
      <tr v-for="item in cubeData"
        :key="item.label"
        :label="item.label"
        :value="item.label">
        <th>{{$t(item.label)}}</th>
        <td>{{item.value}}</td>
      </tr>
    </table>
  </div>
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
<style  lang="less">
  @import '../../../assets/styles/variables.less';
  .overview-edit{
    margin-bottom: 50px;
  }
</style>
