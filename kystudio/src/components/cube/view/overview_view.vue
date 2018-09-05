<template>
  <table class="ksd-table overview-view ksd-mt-20 ">
    <tr class="ksd-tr" v-for="item in cubeData"
      :key="item.label"
      :label="item.label"
      :value="item.label">
      <th>{{$t(item.label)}}</th>
      <td>{{item.value}}</td>
    </tr>
  </table>
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
         {label: 'dimensions', value: this.desc.desc.dimensions.length},
        {label: 'measures', value: this.desc.desc.measures.length}]
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
  @import '../../../assets/styles/variables.less';
  .overview-view {
    tr th {
      width: 168px;
    }
  }
</style>
