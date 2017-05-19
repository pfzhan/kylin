<template>
<el-card class="box-card">
  <el-row>
    <el-col :span="4">{{$t('modelName')}}</el-col>
    <el-col :span="20">{{cubeDesc.model}}</el-col>
  </el-row>
  <el-row>
    <el-col :span="4">{{$t('cubeName')}}</el-col>
    <el-col :span="20">{{cubeDesc.name}}</el-col>
  </el-row>
  <el-row>
    <el-col :span="4">{{$t('notificationEmailList')}}</el-col>
    <el-col :span="20">{{cubeDesc.notify_list}}</el-col>
  </el-row>
  <el-row>
    <el-col :span="4">{{$t('notificationEvents')}}</el-col>
    <el-col :span="20">{{cubeDesc.status_need_notify}}</el-col>
  </el-row>
  <el-row>
    <el-col :span="4">{{$t('description')}}</el-col>
    <el-col :span="20">{{cubeDesc.description}}</el-col>
  </el-row>      
</el-card>
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
<style scoped="">

</style>
