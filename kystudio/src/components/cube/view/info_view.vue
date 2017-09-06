<template>
<div class="cube-info-view ksd-common-table ksd-mt-10">
  <el-row class="tableheader">
    <el-col :span="6" class="left-part"><b>{{$t('modelName')}}</b></el-col>
    <el-col :span="18">{{cubeDesc.model}}</el-col>
  </el-row>
  <el-row class="tableheader">
    <el-col :span="6" class="left-part"><b>{{$t('cubeName')}}</b></el-col>
    <el-col :span="18">{{cubeDesc.name}}</el-col>
  </el-row>
  <el-row class="tableheader">
    <el-col :span="6" class="left-part"><b>{{$t('description')}}</b></el-col>
    <el-col :span="18">{{cubeDesc.desc && cubeDesc.desc.description}}</el-col>
  </el-row>
</div>
</template>

<script>
export default {
  name: 'info',
  props: ['cubeDesc'],
  data () {
    return {
      selected_project: localStorage.getItem('selected_project')
    }
  },
  locales: {
    'en': {modelName: 'Model Name : ', cubeName: 'Cube Name : ', description: 'Description : '},
    'zh-cn': {modelName: '模型名称 : ', cubeName: 'Cube名称 : ', description: '描述 : '}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .cube-info-view{
    .tableheader{
      .el-col{
        text-align: left;
        padding-left: 20px;
      }
      .left-part{
        border-right:solid 1px #393e53;
        text-align: right;
        padding-right: 20px;
      }
    }
  }

</style>
