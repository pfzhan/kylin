<template>
<div class="cube-info-view ksd-common-table ksd-mt-10">
  <el-row class="tableheader">
    <el-col :span="4" class="left-part">{{$t('modelName')}}</el-col>
    <el-col :span="20">{{cubeDesc.model}}</el-col>
  </el-row>
  <el-row class="tableheader">
    <el-col :span="4" class="left-part">{{$t('cubeName')}}</el-col>
    <el-col :span="20">{{cubeDesc.name}}</el-col>
  </el-row>
  <el-row class="tableheader">
    <el-col :span="4" class="left-part">{{$t('notificationEmailList')}}</el-col>
    <el-col :span="20">{{cubeDesc.notify_list}}</el-col>
  </el-row>
  <el-row class="tableheader">
    <el-col :span="4" class="left-part">{{$t('notificationEvents')}}</el-col>
    <el-col :span="20">{{cubeDesc.status_need_notify}}</el-col>
  </el-row>
  <el-row class="tableheader">
    <el-col :span="4" class="left-part">{{$t('description')}}</el-col>
    <el-col :span="20">{{cubeDesc.description}}</el-col>
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
    'en': {modelName: 'Model Name : ', cubeName: 'Cube Name : ', notificationEmailList: 'Notification Email List : ', notificationEvents: 'Notification Events : ', description: 'Description : '},
    'zh-cn': {modelName: '模型名称 : ', cubeName: 'Cube名称 : ', notificationEmailList: '通知邮件列表 : ', notificationEvents: '需通知的事件 : ', description: '描述 : '}
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
