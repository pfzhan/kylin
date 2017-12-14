<template>
  <div id="over-view">
  <div class="cube-info-view ksd-common-table ksd-mt-10">
  <el-row class="tableheader" v-for="d in cubeData" :key="d.value">
    <el-col :span="4" class="left-part"> <b>{{$t(d.label)}}</b></el-col>
    <el-col :span="20"> {{d.value}}</el-col>
  </el-row>
  </div>
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
         {label: 'dimensions', value: this.desc.desc.dimensions.length},
        {label: 'measures', value: this.desc.desc.measures.length}]
      return cubeDetail
    }
  },
  locales: {
    'en': {modelName: 'Model Name:', cubeName: 'Cube Name:', factTable: 'Fact Table:', lookupTable: 'Lookup Table:', dimensions: 'Dimensions:', measures: 'Measures:'},
    'zh-cn': {modelName: '模型名称：', cubeName: 'Cube名称：', factTable: '事实表：', lookupTable: '维度表：', dimensions: '维度：', measures: '度量：'}
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
